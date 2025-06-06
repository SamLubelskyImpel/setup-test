"""Send webhook notifications to clients."""

import logging
from os import environ
from typing import Any
import requests
from utils import get_secret
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def send_webhook_notification(client_secrets: dict, event_content: dict) -> None:
    """Send a webhook notification to the client."""
    url = client_secrets["url"]
    headers = client_secrets.get("headers", {})
    params = client_secrets.get("params", {})

    logger.info(f"Params: {params}")
    logger.info(f"Sending webhook notification: {event_content}")

    response = requests.post(
        url,
        json=event_content,
        headers=headers,
        params=params,
        timeout=30,
    )
    logger.info(f"Webhook response: {response.status_code}")
    response.raise_for_status()


def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        body = record.json_body
        details = body.get("detail", {})

        sort_key = f"SHARED_LAYER_CRM__{details['partner_name']}"
        secret_name = "{}/INS/client-credentials".format("prod" if ENVIRONMENT == "prod" else "test")

        client_secrets = get_secret(secret_name=secret_name, secret_value=sort_key)
        send_webhook_notification(client_secrets, details)

    except Exception as e:
        logger.error(f"Error sending webhook notification: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Send webhook notifications to client."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
