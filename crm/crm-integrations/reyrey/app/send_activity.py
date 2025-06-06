"""Send activity to ReyRey."""

import boto3
import logging
from typing import Any
from os import environ
from json import loads
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from api_wrappers import CrmApiWrapper, CRMApiError, ReyreyApiWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")
crm_api = CrmApiWrapper()


def record_handler(record: SQSRecord):
    """Create activity on ReyRey."""
    logger.info(f"Record: {record}")
    try:
        body = record.json_body
        details = body.get("detail", {})

        activity = crm_api.get_activity(activity_id=details["activity_id"])
        dealer = crm_api.get_dealer_by_idp_dealer_id(idp_dealer_id=details["idp_dealer_id"])

        activity["crm_dealer_id"] = dealer["crm_dealer_id"]
        activity["dealer_timezone"] = dealer["timezone"]

        reyrey_api = ReyreyApiWrapper(activity=activity)

        lead_status = crm_api.get_lead_status(details["lead_id"])
        bad_statuses = reyrey_api.retrieve_bad_lead_statuses()

        if lead_status in bad_statuses:
            logger.warning(f"Lead status is bad for lead ID: {details['lead_id']}")
            return

        reyrey_task_id = reyrey_api.create_activity()
        logger.info(f"Reyrey responded with task ID: {reyrey_task_id}")

        if reyrey_task_id:
            crm_api.update_activity(details["activity_id"], reyrey_task_id)

    except CRMApiError as e:
        logger.warning(f"CRM API error: {e}")
        return
    except Exception as e:
        logger.exception(f"Failed to post activity {details['activity_id']} to ReyRey")
        logger.error(
            f"[SUPPORT ALERT] Failed to Send Activity to ReyRey Focus CRM "
            f"[CONTENT] DealerIntegrationPartnerId: {dealer['dealer_integration_partner_id']}\n"
            f"LeadId: {details['lead_id']}\n"
            f"ActivityId: {details['activity_id']}\n"
            f"ActivityType: {activity['activity_type']}\n"
            f"Traceback: {e}"
        )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity on ReyRey."""
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
