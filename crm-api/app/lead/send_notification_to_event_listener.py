"""Send notification to DA Event listener for the new lead available from CRM API."""
import logging
import os
import json
import requests
from os import environ
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

DA_SECRET_KEY = environ.get("DA_SECRET_KEY")


class EventListenerError(Exception):
    """An exception indicating a failure to send a lead to the DA Event Listener."""
    pass


def send_to_event_listener(lead_id: int) -> None:
    """Send notification to DA Event listener."""
    try:
        listener_secrets = get_secret(
            secret_name="crm-integrations-partner", secret_key=DA_SECRET_KEY
        )

        data = {
            "message": "New Lead available from CRM API",
            "lead_id": lead_id,
        }
        response = requests.post(
            url=listener_secrets["API_URL"],
            headers={"Authorization": listener_secrets["API_TOKEN"]},
            json=data,
            timeout=5,
        )
        logger.info(f"DA Event Listener responded with status: {response.status_code}")
        response.raise_for_status()

    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout occurred calling DA Event Listener for the lead {lead_id}"
        )
        raise EventListenerError(f"Timeout error for the lead {lead_id}")
    except Exception as e:
        message = f"Error sending the lead {lead_id} to DA Event Listener: {e}"
        logger.error(message)
        send_email_notification(message)
        raise EventListenerError(message)


def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        message_body = record['body']
        message = json.loads(message_body)

        lead_id = message['lead_id']

        logger.info(f"Processing lead_id: {lead_id}")
        send_to_event_listener(lead_id)
    except EventListenerError:
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw reyrey lead update data to the unified format."""
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
