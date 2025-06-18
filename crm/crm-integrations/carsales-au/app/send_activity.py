from logging import getLogger
from os import environ
from typing import Any

from api_wrappers import CarsalesApiWrapper, CRMApiError, CrmApiWrapper
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

crm_api = CrmApiWrapper()

API_KEY = "8mb4ax%^tuaab"


def record_handler(record: SQSRecord):
    """Create activity on Carsales."""
    logger.info(f"Record: {record}")
    logger.info(f"Secret key: {API_KEY}")
    try:
        body = record.json_body
        details = body.get("detail", {})

        activity = crm_api.get_activity(details["activity_id"])

        if not activity:
            logger.warning(f"Activity not found for ID: {details['activity_id']}")

        logger.info(f"Activity: {activity}")

        carsales_crm_api = CarsalesApiWrapper(activity=activity)
        carsales_crm_api.create_activity()
    except CRMApiError as e:
        logger.error(f"CRM API Error: {e}")
        raise
    except Exception:
        logger.exception(
            f"Failed to post activity {activity['activity_id']} to Carsales"
        )
        logger.error(
            f"[SUPPORT ALERT] Failed to Send Activity [CONTENT]."
            f"LeadId: {details['lead_id']}"
            f"CRMLeadId: {activity['crm_lead_id']}"
            f"DealerIntegrationPartnerId: {details['idp_dealer_id']}"
            f"ActivityType: {activity['activity_type']}"
            f"ActivityId: {activity['activity_id']}"
        )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity on Carsales."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
        return result

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
