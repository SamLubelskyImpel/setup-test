from os import environ
import boto3
from json import dumps, loads
from datetime import datetime
import logging

sm_client = boto3.client("secretsmanager")
sqs_client = boto3.client("sqs")
logger = logging.getLogger()

ENVIRONMENT = environ.get("ENVIRONMENT")
EVENT_ENRICHER_QUEUE_URL = environ.get("EVENT_ENRICHER_QUEUE_URL")


def get_secret(secret_name, secret_key):
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data


def send_message_to_event_enricher(payload_details):
    """Send message to Event Enricher Queue."""

    payload = {
                "Time": datetime.now().isoformat(),
                "DetailType": "JSON",
                "EventBusName": f"crm-shared-${ENVIRONMENT}-CrmEventBus",
                "Source": "crm-api",
                "Detail": payload_details
            }

    sqs_client.send_message(
        QueueUrl=EVENT_ENRICHER_QUEUE_URL,
        MessageBody=dumps(payload)
    )

    logger.info(f"Message sent to Event Enricher Queue: {payload}")