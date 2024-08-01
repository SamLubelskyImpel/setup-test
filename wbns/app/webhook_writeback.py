"""Send webhook notifications to clients."""

import boto3
import logging
import requests
from json import loads
from os import environ
from typing import Any
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sm_client = boto3.client('secretsmanager')


def get_secret(secret_name: str, secret_value: str) -> dict:
    """Get the secret value from Secrets Manager."""
    try:
        secret = sm_client.get_secret_value(SecretId=secret_name)
        secret = loads(secret["SecretString"])[secret_value]
        return loads(secret)
    except Exception as e:
        logger.error(f"Error getting secret: {e}")
        raise


def send_webhook_notification(client_secrets: dict, event_content: dict) -> None:
    """Send a webhook notification to the client."""
    url = client_secrets["url"]
    headers = client_secrets.get("headers", {})
    params = client_secrets.get("params", {})

    logger.info(f"Headers: {headers}")
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

    if response.status_code != 200:
        logger.error(f"Error response text: {response.text}")
        response.raise_for_status()


def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        message = loads(record["body"])
        events = message["detail"]['events']

        for event in events:
            logger.info(f"Event: {event}")

            event_id = event['event_id']
            event_content = event['event_content']
            event_content["event_id"] = event_id

            product_name = event['product_name']
            client_id = event['client_id']
            sort_key = f"{product_name}__{client_id}"

            client_secrets = get_secret(secret_name=f"{ENVIRONMENT}/WBNS/client-credentials", secret_value=sort_key)

            send_webhook_notification(client_secrets, event_content)

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
