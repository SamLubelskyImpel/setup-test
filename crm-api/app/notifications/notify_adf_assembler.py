"""Forward event to ADF Assembler from CRM API."""
import logging
import os
import json
import requests
from typing import Any
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from utils import send_email_notification, get_secret

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())


class ADFAssemblerError(Exception):
    """An exception indicating a failure to send an event to the ADF Assembler."""
    pass


def send_to_adf_assembler(event: Any) -> None:
    """Send event to ADF Assembler."""
    try:
        assembler_secrets = get_secret(
            secret_name="adf-assembler", secret_key="create_adf"
        )
        api_url, api_key = assembler_secrets.values()

        response = requests.post(
            url=api_url,
            data=json.dumps(event),
            headers={
                "x_api_key": api_key,
                "action_id": "create_adf",
                'Content-Type': 'application/json'
            }
        )
        logger.info(f"ADF Assembler responded with status: {response.status_code}")
        response.raise_for_status()

    except requests.exceptions.Timeout:
        logger.error("Timeout occurred calling ADF Assembler for the event")
        raise ADFAssemblerError("Timeout error for event.")
    except Exception as e:
        message = f"Error sending the event to ADF Assembler: {e}"
        logger.error(message)
        send_email_notification(message, subject="ADF Assembler Failure Alert")
        raise ADFAssemblerError(message)


def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        message_body = record['body']
        event = json.loads(message_body)

        logger.info(f"Processing event: {event}")
        send_to_adf_assembler(event)
    except ADFAssemblerError:
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Process and syndicate event to ADF Assembler."""
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
