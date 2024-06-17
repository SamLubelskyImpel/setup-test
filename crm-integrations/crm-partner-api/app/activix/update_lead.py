import logging
import boto3
import hmac
import hashlib
import json
from json import dumps, loads
import requests
from datetime import datetime
from os import environ
from typing import Any
from uuid import uuid4

BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
ENVIRONMENT = environ.get("ENVIRONMENT")
PARTNER_ID = environ.get("PARTNER_ID")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")


def create_signature(api_key, body):
    """
    Create an HMAC SHA256 signature using the API key and body (JSON data).
    """
    body_json = json.dumps(body, separators=(',', ':'))
    key_bytes = api_key.encode()
    message_bytes = body_json.encode()
    hmac_obj = hmac.new(key_bytes, message_bytes, hashlib.sha256)
    return hmac_obj.hexdigest()


def get_secrets():
    """Get CRM API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret["SecretString"])[PARTNER_ID]
    secret_data = loads(secret)

    return secret_data["api_key"]


def get_api_keys(crm_dealer_id: str) -> str:
    """Get Activix secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/activix"
    )
    secret = loads(secret["SecretString"])[crm_dealer_id]
    secret_data = loads(secret)

    return [secret_data["leadStatusUpdate"], secret_data["leadSalespersonUpdate"]]


def get_dealers(integration_partner_name: str) -> Any:
    """Get dealers from CRM API."""
    api_key = get_secrets()
    url = f"https://{CRM_API_DOMAIN}/dealers"

    response = requests.get(
        url=url,
        headers={"partner_id": PARTNER_ID, "x_api_key": api_key},
        params={"integration_partner_name": integration_partner_name},
    )
    logger.info(f"CRM API responded with: {response.status_code}")
    response.raise_for_status()
    return response.json()


def save_raw_lead(lead: str, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = "%Y/%m/%d/%H/%M"
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw_updates/activix/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving activix lead to {s3_key}")
    s3_client.put_object(
        Body=lead,
        Bucket=BUCKET,
        Key=s3_key,
    )


def lambda_handler(event: Any, context: Any) -> Any:
    """This API handler takes the Json sent by momentum and puts the raw Json into the S3 bucket."""
    try:
        logger.info(f"Event: {event}")
        body = loads(event["body"])
        signature = event["headers"]["X-Activix-Signature"]

        activix_dealer_list = get_dealers("ACTIVIX")
        crm_dealer_id = str(body["account_id"])

        for dealer in activix_dealer_list:
            if dealer["crm_dealer_id"] == crm_dealer_id:
                product_dealer_id = dealer["product_dealer_id"]
                break
        else:
            logger.error(f"Dealer {crm_dealer_id} not found in active dealers.")
            return {
                "statusCode": 422,
                "body": dumps({
                    "error": "Unknown Account (Dealer)."
                }),
            }

        api_keys = get_api_keys(crm_dealer_id)
        signatures = [create_signature(api_key, body) for api_key in api_keys]
        if signature not in signatures:
            logger.error("Invalid signature.")
            return {
                "statusCode": 401,
                "body": dumps({
                    "error": "This request is unauthorized. The authorization credentials are missing or are wrong."
                }),
            }

        logger.info(f"Lead update received for dealer: {product_dealer_id}")
        logger.info(f"Lead body: {body}")
        save_raw_lead(dumps(body), product_dealer_id)

        return {
            "statusCode": 200
        }

    except Exception as e:
        error_message = str(e)
        logger.error(f"Error getting Activix lead update: {error_message}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": dumps({"error": "Internal Server Error. Please contact Impel support."})
        }
