from typing import Any
from os import environ
from logging import getLogger
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from api_wrappers import CarsalesApiWrapper, CrmApiWrapper, CRMApiError

CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")

crm_api = CrmApiWrapper()

def record_handler(record: SQSRecord):
    """Create activity on Carsales."""
    logger.info(f"Record: {record}")

    try:
        body = record.json_body
        details = body.get("detail", {})
        
        activity = crm_api.get_activity(details["activity_id"])
        lead = crm_api.get_lead(details["lead_id"])

        if not activity:
            raise ValueError(f"Activity not found for ID: {details['activity_id']}")

        if not lead:
            raise ValueError(f"Lead not found for ID: {details['lead_id']}")
        
        activity['crm_lead_id'] = lead['crm_lead_id']

        logger.info(f"Activity: {activity}")
        logger.info(f"Lead: {lead}")

        carsales_crm_api = CarsalesApiWrapper(activity=activity)
        carsales_crm_api.create_activity()
    except CRMApiError as e:
        logger.error(f"CRM API Error: {e}")
        raise
    except Exception as e:
        logger.exception(f"Failed to post activity {activity['activity_id']} to Carsales")
        logger.error(f"[SUPPORT ALERT] Failed to Send Activity [CONTENT]."
                     f"LeadId: {details['lead_id']}"
                     f"CRMLeadId: {lead['crm_lead_id']}"
                     f"DealerIntegrationPartnerId: {activity['dealer_integration_partner_id']}"
                     f"ActivityId: {activity['activity_id']}"
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
