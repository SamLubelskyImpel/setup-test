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

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = f"integrations-us-east-1-{'prod' if ENVIRONMENT == 'prod' else 'test'}"
CE_TOPIC = environ.get("CE_TOPIC")


class TekionWrapper:
    def __init__(self, dealer_id, end_dt_str=None):
        self.base_url = (
            "https://openapi.tekioncloud.com"
            if ENVIRONMENT == "prod"
            else "https://api.tekioncloud.xyz"
        )
        self.dealer_id = dealer_id
        self.access_key, self.secret_key, self.client_id = self._get_secrets()
        self._expire_datetime = None
        self._token = None
        if end_dt_str:
            self.end_dt = datetime.strptime(end_dt_str, "%Y-%m-%dT%H:%M:%S")
        else:
            self.end_dt = datetime.utcnow()

    def _get_secrets(self) -> Tuple[str, str, str]:
        """Retrieve Tekion access key and secret key."""
        secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'stage'}/tekion"
        try:
            secret = loads(
                boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)[
                    "SecretString"
                ]
            )
            return secret["TEKION_ACCESS_KEY"], secret["TEKION_SECRET_KEY"], secret["TEKION_CLIENT_ID"]
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
        token_url = f"{self.base_url}/auth/v1/oauth2/token"
        headers = {
            "accept": "application/json",
            "client_id": self.client_id,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "access-key": self.access_key,
            "secret-key": self.secret_key
        }
        resp = requests.post(token_url, headers=headers, data=data)
        resp.raise_for_status()
        try:
            resp_data = resp.json()
            token = resp_data["access_token"]
            # Refreshes token if request within 10 seconds of expirey time for server leeway.
            expire_seconds = int(resp_data["expires_in"]) - 10
            expire_datetime = datetime.utcnow() + timedelta(
                seconds=expire_seconds
            )
            return token, expire_datetime
        except JSONDecodeError:
            logger.exception(f"Unable to parse response {resp}")
            raise

    def _call_tekion(self, path, next_fetch_key=""):
        """Retrieve Tekion data."""
        url = f"{self.base_url}/{path}"
        end_date = self.end_dt.replace(hour=0, minute=0, second=0)
        start_date = end_date - timedelta(days=1)
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)
        url += f"?startTime={start_time}&endTime={end_time}"
        if next_fetch_key:
            url += f"&nextFetchKey={next_fetch_key}"
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "client_id": self.client_id,
            "dealerid": self.dealer_id,
            "accept": "application/json",
        }
        resp = requests.get(url, headers=headers)

        if resp.status_code == 403:
            logger.info(f"403 returned from {url}, dealer_id {self.dealer_id}")
            boto3.client("sns").publish(
                TopicArn=CE_TOPIC,
                Message=f"Tekion API returned 403 for request {url}",
            )

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

    def get_repair_orders(self, next_fetch_key=""):
        """Retrieve Tekion Repair Order."""
        return self._call_tekion("api/v2/repairorder", next_fetch_key=next_fetch_key)

    def get_deals(self, next_fetch_key=""):
        """Retrieve Deals by dealer id"""
        return self._call_tekion("api/v2.2/deal", next_fetch_key=next_fetch_key)

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
