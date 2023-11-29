"""Get active dealers."""

import boto3
import logging
import requests
from os import environ
from json import dumps, loads
from typing import Any
from datetime import datetime, timedelta

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_API_URL = environ.get("CRM_API_URL")
PARTNER_ID = environ.get("PARTNER_ID")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")
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
    """Get dealers from CRM API."""
    api_key = get_secrets()
    response = requests.get(
        url=f"{CRM_API_URL}dealers",
        headers={
            "partner_id": PARTNER_ID,
            "x_api_key": api_key
        },
        params={
            "integration_partner_name": integration_partner_name
        }
    )
    logger.info(f"CRM API responded with: {response.status_code}")
    if response.status_code != 200:
        logger.error(f"Error getting dealers {integration_partner_name}: {response.text}")
        raise

    return response.json()


def send_dealer_event(integration_partner_name: str, dealers: list, start_time: str, end_time: str) -> Any:
    """Send dealer event to invoke data pull."""
    s3_key = f"configurations/{ENVIRONMENT}_{integration_partner_name}.json"
    try:
        queue_url = loads(
            s3_client.get_object(
                Bucket=BUCKET,
                Key=s3_key
            )['Body'].read().decode('utf-8')
        )["invoke_dealer_queue_url"]
    except Exception as e:
        logger.error(f"Failed to retrieve queue url from S3 config. Partner: {integration_partner_name}, {e}")
        raise

    for dealer in dealers:
        dealer.update({
            "start_time": start_time,
            "end_time": end_time
        })
        logger.info(f"Sending message to queue: {dealer}")
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(dealer)
        )


def lambda_handler(event: Any, context: Any) -> Any:
    """Get active dealers."""
    logger.info(f"Event: {event}")

    integration_partner_name = event["impel_integration_partner_name"]
    try:
        current_time = datetime.utcnow()
        start_time = (current_time - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        dealers = get_dealers(integration_partner_name)
        if not dealers:
            logger.error(f"No active dealers found for {integration_partner_name}")
            return

        send_dealer_event(integration_partner_name, dealers, start_time, end_time)

    except Exception as e:
        logger.error(f"Error occured getting active dealers: {e}")
        raise
