"""Tekion API Wrapper."""
import logging
import requests
import boto3
from datetime import datetime, timedelta
from json import JSONDecodeError, loads, dumps
from typing import Tuple
from os import environ
from botocore.exceptions import ClientError
from utils.token import get_token_from_s3, invoke_refresh_token

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
CE_TOPIC = environ.get("CE_TOPIC")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class TekionWrapper:
    def __init__(self, dealer_id, end_dt_str=None):
        self.dealer_id = dealer_id
        self.app_id, self.secret_key, self.base_url = self._get_secrets()
        self._expire_datetime = None
        self._token = None
        if end_dt_str:
            self.end_dt = datetime.strptime(end_dt_str, "%Y-%m-%dT%H:%M:%S")
        else:
            self.end_dt = datetime.utcnow()

    def _get_secrets(self) -> Tuple[str, str, str]:
        """Retrieve Tekion access key and secret key."""
        secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'stage'}/dms-integrations-partner"
        try:
            secret = loads(loads(
                boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)[
                    "SecretString"
                ]
            )["TEKION_V4"])

            return secret["app_id"], secret["secret_key"], secret["url"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise RuntimeError(f"Secret '{secret_id}' not found")
            raise

    def _get_token(self):
        token = get_token_from_s3()

        if not token:
            invoke_refresh_token(self.dealer_id)
            raise Exception("Invalid auth token")

        return token

    def authorize(self):
        """Retrieve Tekion Bearer Token."""
        token_url = f"{self.base_url}/openapi/public/tokens"
        headers = {
            "accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "app_id": self.app_id,
            "secret_key": self.secret_key
        }
        resp = requests.post(token_url, headers=headers, data=data)
        resp.raise_for_status()
        try:
            resp_data = resp.json()["data"]
            token = resp_data["access_token"]
            # Refreshes token if request within 10 seconds of expire time for server leeway.
            expire_seconds = int(resp_data["expire_in"]) - 10
            expire_datetime = datetime.utcnow() + timedelta(
                seconds=expire_seconds
            )
            return token, expire_datetime
        except JSONDecodeError:
            logger.exception(f"Unable to parse response {resp}")
            raise

    def _alert_ce(self, url: str, status: int):
        logger.error(f"{status} returned from {url}, dealer_id {self.dealer_id}")
        boto3.client("sns").publish(
            TopicArn=CE_TOPIC,
            Message=f"[TEKION APC DMS] API returned {status} for request {url}",
        )

    def _call_tekion(self, path, next_fetch_key="", params: dict={}, set_date_filter=True):
        """Retrieve Tekion data."""
        url = f"{self.base_url}/{path}"
        url_params = params
        if set_date_filter:
            end_date = self.end_dt.replace(hour=0, minute=0, second=0)
            start_date = end_date - timedelta(days=1)
            start_time = int(start_date.timestamp() * 1000)
            end_time = int(end_date.timestamp() * 1000)
            url_params["startTime"] = start_time
            url_params["endTime"] = end_time
        if next_fetch_key:
            url_params["nextFetchKey"] = next_fetch_key
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "dealer_id": self.dealer_id,
            "app_id": self.app_id,
            "accept": "application/json",
        }
        resp = requests.get(url, headers=headers, params=params)
        logger.info(f"Url: {url} Params: {params} Status: {resp.status_code} {resp.text}")

        if resp.status_code != 200:
            logger.error(f"Tekion responded with {resp.status_code} {resp.text}")

        if resp.status_code in (429, 403):
            self._alert_ce(url, resp.status_code)

        resp.raise_for_status()

        try:
            resp_json = resp.json()
            meta = resp_json["meta"]

            if meta["status"] != "success":
                raise RuntimeError(
                    f"{path} returned {resp.status_code} but metadata of {meta}"
                )
            if meta["currentPage"] < meta["pages"] and meta["nextFetchKey"]:
                return resp_json["data"] + self._call_tekion(
                    path, next_fetch_key=meta["nextFetchKey"]
                )
            else:
                return resp_json["data"]
        except JSONDecodeError:
            logger.exception(f"Unable to parse {path} response {resp}")
            raise

    def _call_tekion_v4(self, path, next_fetch_key="", params: dict={}, set_date_filter=True):
        """Retrieve Tekion data from api v4.0.0"""
        url = f"{self.base_url}/{path}"
        url_params = params
        if set_date_filter:
            end_date = self.end_dt.replace(hour=0, minute=0, second=0)
            start_date = end_date - timedelta(days=1)
            url_params["createdStartTime"] = int(start_date.timestamp() * 1000)
            url_params["createdEndTime"] = int(end_date.timestamp() * 1000)
        if next_fetch_key:
            url_params["nextFetchKey"] = next_fetch_key
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "dealer_id": self.dealer_id,
            "app_id": self.app_id,
            "accept": "application/json",
        }
        resp = requests.get(url, headers=headers, params=params)
        logger.info(f"Url: {url} Params: {params} Status: {resp.status_code} {resp.text}")

        if resp.status_code != 200:
            logger.error(f"Tekion responded with {resp.status_code} {resp.text}")

        if resp.status_code in (429, 403):
            self._alert_ce(url, resp.status_code)
            
        resp.raise_for_status()
   
        try:
            resp_json = resp.json()
            meta = resp_json["meta"]

            if meta["status"] != "success":
                raise RuntimeError(
                    f"{path} returned {resp.status_code} but metadata of {meta}"
                )
            if meta.get("nextFetchKey", None):
                return resp_json["data"] + self._call_tekion(
                    path, next_fetch_key=meta["nextFetchKey"]
                )
            else:
                return resp_json["data"]
        except JSONDecodeError:
            logger.exception(f"Unable to parse {path} response {resp}")
            raise

    def get_repair_orders_v3(self, next_fetch_key=""):
        """Retrieve Tekion Repair Order."""
        return self._call_tekion("openapi/v3.1.0/repair-orders", next_fetch_key=next_fetch_key)

    def get_deals_v4(self, next_fetch_key=""):
        """Retrieve Deals by dealer id"""
        return self._call_tekion_v4("openapi/v4.0.0/deals", next_fetch_key=next_fetch_key)

    def get_appointments_v3(self, next_fetch_key=""):
        """Retrieve Appointments by dealer id"""
        return self._call_tekion("openapi/v3.1.0/appointments", next_fetch_key=next_fetch_key)

    def get_customer_v3(self, id: str):
        """Retrieve Customer by id."""
        return self._call_tekion(f"openapi/v3.1.0/customers", params={ "id": id }, set_date_filter=False)

    def get_customer_v4(self, id: str):
        """Retrieve Customer by id."""
        return self._call_tekion_v4(f"openapi/v4.0.0/customers/", params={ "customerId": id }, set_date_filter=False)

    def get_employee_v4(self, id: str):
        """Retrieve User (Employee) by id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/users/{id}", set_date_filter=False)

    def get_deal_customers_v4(self, deal_id: str):
        """Retrieve all Customers associated with a Deal by Deal id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/customers", set_date_filter=False)

    def get_deal_payment_v4(self, deal_id: str):
        """Retrieve Payment Details associated with a Deal by Deal id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/deal-payment", set_date_filter=False)

    def get_deal_service_contracts_v4(self, deal_id: str):
        """Retrieve all Service Contracts (fnis) associated with a Deal by Deal id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/deal-payment/fnis", set_date_filter=False)

    def get_deal_trade_ins_v4(self, deal_id: str):
        """Retrieve all Trade Ins associated with a Deal by Deal id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/trade-ins", set_date_filter=False)

    def get_deal_gross_details_v4(self, deal_id: str):
        """Retrieve Gross Details associated with a Deal by Deal id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/gross-details", set_date_filter=False)
        
    def get_deal_vehicles_v4(self, deal_id: str):
        """Retrieve all Vehicles associated with a Deal by Deal id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/vehicles", set_date_filter=False)

    def get_vehicle_warranties_v4(self, vehicle_inventory_id: str):
        """Retrieve all Warranties associated with a Vehicle by Vehicle Inventory id"""
        return self._call_tekion_v4(f"openapi/v4.0.0/vehicle-inventory/{vehicle_inventory_id}/warranties", set_date_filter=False)

    def get_deal_assignees_v4(self, deal_id: str):
        """
        Retrieve all Sales Associates associated with a Deal by Deal id.
        Further API call is required for specific information about assignees
        """
        return self._call_tekion_v4(f"openapi/v4.0.0/deals/{deal_id}/assignees", set_date_filter=False)

    def upload_data(self, api_data, key):
        """Upload API data to S3."""
        if api_data:
            s3_client = boto3.client("s3")
            response = s3_client.put_object(
                Bucket=INTEGRATIONS_BUCKET,
                Key=key,
                Body=dumps(api_data),
                ContentType="application/json",
            )
            if not response["ETag"]:
                raise RuntimeError(f"Unexpected s3 response {response}")
            logger.info(f"Uploaded {key}")
        else:
            logger.warning(f"No data {api_data} to upload for {key}")
