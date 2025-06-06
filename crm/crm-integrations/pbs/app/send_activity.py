"""Send activity to PBS."""

from typing import Any
from os import environ
from json import loads
from boto3 import client
from logging import getLogger
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from api_wrappers import CRMAPIWrapper, PbsApiWrapper, CRMApiError

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")

crm_api = CRMAPIWrapper()


def record_handler(record: SQSRecord):
    """Create activity on PBS CRM."""
    logger.info(f"Record: {record}")
    activity = {}

    try:
        body = record.json_body
        details = body.get("detail", {})

        salesperson = crm_api.get_salesperson(details["lead_id"])
        activity = crm_api.get_activity(details["activity_id"])
        dealer = crm_api.get_dealer_by_idp_dealer_id(details["idp_dealer_id"])
        lead = crm_api.get_lead(details["lead_id"])

        activity["crm_dealer_id"] = dealer["crm_dealer_id"]
        activity["dealer_timezone"] = dealer["timezone"]
        activity["dealer_integration_partner_metadata"] = dealer["metadata"]

        if not activity:
            raise ValueError(f"Activity not found for ID: {details['activity_id']}")

        consumer = crm_api.get_consumer(lead["consumer_id"])

        if not salesperson or not consumer:
            logger.warning(f"Missing required data: Salesperson or Consumer not found for lead_id: {details['lead_id']} or consumer_id: {lead['consumer_id']}")
            return

        logger.info(f"Activity: {activity}, Salesperson: {salesperson}, Consumer: {consumer}")

        pbs_crm_api = PbsApiWrapper(activity=activity, salesperson=salesperson, consumer=consumer)
        pbs_activity_id = pbs_crm_api.create_activity()

        logger.info(f"PBS responded with activity ID: {pbs_activity_id}")
        crm_api.update_activity(details["activity_id"], pbs_activity_id)

    except CRMApiError:
        return
    except Exception as e:
        logger.exception(f"Failed to post activity {details['activity_id']} to PBS")
        logger.error(
            f"[SUPPORT ALERT] Failed to Send Activity [CONTENT] "
            f"IdpDealerId: {activity.get('idp_dealer_id', '')}\n"
            f"LeadId: {details['lead_id']}\n"
            f"ActivityId: {details['activity_id']}\n"
            f"ActivityType: {activity.get('activity_type', '')}\n"
            f"Traceback: {e}"
        )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Handle the SQS event and process each record."""
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
