"""Tekion API Wrapper."""
from datetime import datetime, timedelta
from json import JSONDecodeError, loads, dumps
from typing import Tuple
from os import environ
import logging
import requests

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = f"integrations-us-east-1-{'prod' if ENVIRONMENT == 'prod' else 'test'}"


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
        secret_id = f"{ENVIRONMENT}/tekion"
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
        """Retrieve Tekion Bearer Token."""
        if (
            not self._token
            or not self._expire_datetime
            or self._expire_datetime <= datetime.utcnow()
        ):
            token_url = f"{self.base_url}/auth/v1/oauth2/token"
            headers = {
                "accept": "application/json",
                "client_id": self.client_id,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            data = f"access-key={self.access_key}&secret-key={self.secret_key}"
            resp = requests.post(token_url, headers=headers, data=data)
            resp.raise_for_status()
            try:
                resp_data = resp.json()
                self._token = resp_data["access_token"]
                # Refreshes token if request within 10 seconds of expirey time for server leeway.
                expire_seconds = int(resp_data["expires_in"] / 1000) - 10
                self._expire_datetime = datetime.utcnow() + timedelta(
                    seconds=expire_seconds
                )
            except JSONDecodeError:
                logger.exception(f"Unable to parse response {resp}")
                raise
        return self._token
    
    def get_repair_orders(self, next_fetch_key=""):
        """Retrieve Tekion Repair Order."""
        repair_order_url = f"{self.base_url}/api/v2/repairorder"
        end_date = self.end_dt.replace(hour=0, minute=0, second=0)
        start_date = end_date - timedelta(days=1)
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)
        repair_order_url += f"?startTime={start_time}&endTime={end_time}"
        if next_fetch_key:
            repair_order_url += f"&nextFetchKey={next_fetch_key}"
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "client_id": self.client_id,
            "dealerid": self.dealer_id,
            "accept": "application/json",
        }
        resp = requests.get(repair_order_url, headers=headers)
        resp.raise_for_status()

        try:
            resp_json = resp.json()
            meta = resp_json["meta"]

            if meta["status"] != "success":
                raise RuntimeError(
                    f"Repair order returned {resp.status_code} but metadata of {meta}"
                )
            if meta["currentPage"] < meta["pages"] and meta["nextFetchKey"]:
                return resp_json["data"] + self.get_repair_order(
                    next_fetch_key=meta["nextFetchKey"]
                )
            else:
                return resp_json["data"]
        except JSONDecodeError:
            logger.exception(f"Unable to parse repair order response {resp}")
            raise

    def get_deals(self, next_fetch_key=""):
        """Retrieve Deals by dealer id"""
        deal_url = f"{self.base_url}/api/v2/deal"
        end_date = self.end_dt.replace(hour=0, minute=0, second=0)
        start_date = end_date - timedelta(days=1)
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)
        deal_url += f"?startTime={start_time}&endTime={end_time}"
        if next_fetch_key:
            deal_url += f"&nextFetchKey={next_fetch_key}"
        token = self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "client_id": self.client_id,
            "dealerid": self.dealer_id,
            "accept": "application/json",
        }
        resp = requests.get(deal_url, headers=headers)
        resp.raise_for_status()

        try:
            resp_json = resp.json()
            meta = resp_json["meta"]

            if meta["status"] != "success":
                raise RuntimeError(
                    f"Deals returned {resp.status_code} but metadata of {meta}"
                )
            if meta["currentPage"] < meta["pages"] and meta["nextFetchKey"]:
                return resp_json["data"] + self.get_deals(
                    next_fetch_key=meta["nextFetchKey"]
                )
            else:
                return resp_json["data"]
        except JSONDecodeError:
            logger.exception(f"Unable to parse deals response {resp}")
            raise

    def upload_data(self, api_data, key_path):
        """Upload API data to S3."""
        filename = self.end_dt.strftime("%Y%m%dT%H_%M_%S")
        key = f"{key_path}/{filename}.json"
        s3_client = boto3.client("s3")
        response = s3_client.put_object(
            Bucket=INTEGRATIONS_BUCKET,
            Key=key,
            Body=dumps(api_data),
            ContentType="application/json",
        )
        if not response["ETag"]:
            raise RuntimeError(f"Unexpected s3 response {response}")
