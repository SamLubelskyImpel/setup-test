from os import environ
import boto3
from json import dumps
from datetime import datetime
import logging

sqs_client = boto3.client("sqs")
logger = logging.getLogger()

ENVIRONMENT = environ.get("ENVIRONMENT")
EVENT_ENRICHER_QUEUE_URL = environ.get("EVENT_ENRICHER_QUEUE_URL")


def send_message_to_event_enricher(payload_details):
    """Send message to Event Enricher Queue."""

    payload = {
                "Time": datetime.now().isoformat(),
                "DetailType": "JSON",
                "EventBusName": f"crm-shared-{ENVIRONMENT}-CrmEventBus",
                "Source": "crm-api",
                "Detail": payload_details
            }

    sqs_client.send_message(
        QueueUrl=EVENT_ENRICHER_QUEUE_URL,
        MessageBody=dumps(payload)
    )

    logger.info(f"Message sent to Event Enricher Queue: {payload}")
