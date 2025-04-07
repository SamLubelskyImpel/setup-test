"""This module is responsible for querying VDP data."""

import logging
from os import environ
from json import loads
import requests

import boto3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
VDP_API_DOMAIN = environ.get("VDP_API_DOMAIN")
VDP_API_SECRET_KEY = environ.get("VDP_API_SECRET_KEY")

secret_client = boto3.client("secretsmanager")


class VDPApiWrapper:
    """VDP API Wrapper."""
    def __init__(self) -> None:
        self.partner_id = VDP_API_SECRET_KEY
        self.api_key = self.get_secrets()

    def get_secrets(self):
        """This function retrieves the API key from AWS Secrets Manager."""
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/vdp-api"
        )
        secret = loads(secret["SecretString"])[VDP_API_SECRET_KEY]
        secret_data = loads(secret)

        return secret_data["api_key"]

    def __run_get(self, endpoint: str, params: dict = None):
        """This function runs a GET request to the VDP API with optional query parameters."""
        response = requests.get(
            url=f"https://{VDP_API_DOMAIN}/{endpoint}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            },
            params=params,
            timeout=15,
        )
        return response

    def get_vdp_data(self, vehicle_data, impel_dealer_id):
        """This function calls the VDP API to get the VDP data."""
        try:
            identification_data = vehicle_data.get("Identification", [])
            vin = next(
                (item["Value"] for item in identification_data if item["Type"] == "VIN"),
                None,
            )
            stock_no = next(
                (item["Value"] for item in identification_data if item["Type"] == "StockNumber"),
                None,
            )

            if not vin and not stock_no:
                raise ValueError("No VIN or StockNumber found in the Identification data.")

            # Call the VDP Service API
            vdp_response = self.__run_get(
                endpoint="vdp",
                params={
                    "impel_dealer_id": impel_dealer_id,
                    "vin": vin,
                    "stock_number": stock_no,
                },
            )

            if vdp_response.status_code == 200:
                return vdp_response.json()

            if vdp_response.status_code == 404:
                logger.warning(f"VDP data not found for VIN: {vin} or StockNumber: {stock_no}")
                return {}

            vdp_response.raise_for_status()
        except ValueError as e:
            logger.warning(f"Failed to retrieve VDP data: {e}")
            return {}
        except Exception as e:
            logger.exception(f"Failed to retrieve VDP data: {e}")
            raise
