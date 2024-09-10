import logging
import boto3
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


def get_secrets():
    """Get CRM API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret["SecretString"])[PARTNER_ID]
    secret_data = loads(secret)

    return secret_data["api_key"]


def get_dealers(integration_partner_name: str) -> Any:
    """Get active dealers from CRM API."""
    api_key = get_secrets()
    response = requests.get(
        url=f"https://{CRM_API_DOMAIN}/dealers",
        headers={"partner_id": PARTNER_ID, "x_api_key": api_key},
        params={"integration_partner_name": integration_partner_name},
    )
    logger.info(f"CRM API responded with: {response.status_code}")
    if response.status_code != 200:
        logger.error(
            f"Error getting dealers {integration_partner_name}: {response.text}"
        )
        raise

    dealers = response.json()

    # Filter by active Sales AI dealers
    dealers = list(filter(lambda dealer: dealer.get('is_active_salesai', False), dealers))

    return dealers


def save_raw_lead(lead: str, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = "%Y/%m/%d/%H/%M"
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/momentum/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving momentum lead to {s3_key}")
    s3_client.put_object(
        Body=lead,
        Bucket=BUCKET,
        Key=s3_key,
    )


def lambda_handler(event: Any, context: Any) -> Any:
    """This API handler takes the JSON sent by Momentum and puts the raw JSON into the S3 bucket."""
    try:
        logger.info(f"Event: {event}")

        body = loads(event["body"])
        momentum_dealer_list = get_dealers("MOMENTUM")
        crm_dealer_id = body["dealerID"]

        for dealer in momentum_dealer_list:
            if dealer["crm_dealer_id"] == crm_dealer_id:
                product_dealer_id = dealer["product_dealer_id"]
                break
        else:
            logger.error(f"Dealer {crm_dealer_id} not found in active SalesAI dealers.")
            return {
                "statusCode": 401,
                "body": dumps({
                    "error": "This request is unauthorized. The authorization credentials are missing or are wrong. For example if the partner_id or the x_api_key provided in the header are wrong/missing. This error can also occur if the dealerID provided hasn't been configured with Impel."
                }),
            }

        logger.info(f"New lead received for dealer: {product_dealer_id}")
        logger.info(f"Lead body: {body}")
        save_raw_lead(dumps(body), product_dealer_id)

        return {"statusCode": 200}

    except Exception as e:
        logger.error(f"Error getting Momentum create lead: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "Internal Server Error. Please contact Impel support."}),
        }
