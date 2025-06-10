"""Send webhook notifications to clients."""

import logging
from os import environ
from typing import Any
import requests
from utils import get_secret
from botocore.exceptions import ClientError
from api_wrappers import CrmApiWrapper, CRMApiError
from enum import Enum
from uuid import uuid4
from datetime import datetime, timezone
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")

crm_api = CrmApiWrapper()
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class SecretNotFoundError(Exception):
    """Exception raised when the secret is not found."""
    pass


class Event(Enum):
    Created = 'created'
    Updated = 'updated'


class Resource(Enum):
    Lead = 'lead'
    Activity = 'activity'


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

        try:
            client_secrets = get_secret(secret_name=secret_name, secret_value=sort_key)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"Secret not found: {secret_name}")
                raise SecretNotFoundError(f"Secret {secret_name} not found.") from e
            else:
                raise

        resource = Resource.Activity if 'activity' in details['event_type'].lower() else Resource.Lead
        event = Event.Created if 'created' in details['event_type'].lower() else Event.Updated
        event_type = f"shared-layer.crm.{'chatai' if details['source_application'] == 'CHAT_AI' else 'salesai'}.{resource.value}.{event.value}"

        event_content = {}
        event_content["created_ts"] = datetime.now(timezone.utc).isoformat()
        event_content["message"] = details["event_type"]
        event_content["lead_id"] = details["lead_id"]

        if details.get('activity_id'):
            activity = crm_api.get_activity(details["activity_id"])

            if not activity:
                logger.warning(f"Activity not found for ID: {details['activity_id']}")
                raise ValueError(f"Activity not found for ID: {details['activity_id']}")

            event_content["activity_id"] = details["activity_id"]
            event_content["dealer_id"] = activity.get("dealer_id")
            event_content["activity_type"] = activity.get("activity_type")

        elif details.get('consumer_id'):
            consumer = crm_api.get_consumer(details["consumer_id"])

            if not consumer:
                logger.warning(f"Consumer not found for ID: {details['consumer_id']}")
                raise ValueError(f"Consumer not found for ID: {details['consumer_id']}")

            event_content["consumer_id"] = details["consumer_id"]
            event_content["dealer_id"] = consumer.get("dealer_id")

        else:
            logger.warning("No activity_id or consumer_id found in details.")
            raise ValueError("No activity_id or consumer_id found in details.")

        event_body = {
            "event_id": str(uuid4()),
            "event_type": event_type,
            "client_id": details["partner_name"],
            "product_name": "SHARED_LAYER_CRM",
            "event_content": event_content
        }

        send_webhook_notification(client_secrets, event_body)

    except SecretNotFoundError as e:
        logger.warning(f"Missing secret: {e}")

    except CRMApiError as e:
        logger.error(f"CRM API error: {e}")
        raise

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
