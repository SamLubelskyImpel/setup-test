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

from api_wrappers import CrmApiWrapper, ActivixApiWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())



def record_handler(record: SQSRecord):
    """Create activity on Activix."""
    logger.info(f"Record: {record}")

    secret_client = client("secretsmanager")
    crm_api = CrmApiWrapper()
    try:
        body = record.json_body
        details = body.get("detail", {})

        idp_dealer_id = details.get("idp_dealer_id")
        lead_id = details.get("lead_id")
        activity_id = details.get("activity_id")

        activity = crm_api.get_activity(activity_id)
        dealer = crm_api.get_dealer_by_idp_dealer_id(idp_dealer_id)

        activity = {
            "lead_id": lead_id,
            "crm_lead_id": activity.get("crm_lead_id", None),
            "dealer_integration_partner_id": dealer.get("dealer_integration_partner_id", None),
            "crm_dealer_id": dealer.get("crm_dealer_id", None),
            "consumer_id": None,
            "crm_consumer_id": activity.get("crm_consumer_id", None),
            "dealer_integration_partner_metadata": dealer.get("metadata", None),
            "activity_id": activity_id,
            "notes": activity.get("notes", None),
            "activity_due_ts": activity.get("activity_due_ts", None),
            "activity_requested_ts": activity.get("activity_requested_ts", None),
            "dealer_timezone": dealer.get("timezone", None),
            "activity_type": activity.get("activity_type", None),
            "contact_method": activity.get("contact_method", None),
        }
        logger.info(f"Activity: {activity}")

        salesperson = crm_api.get_salesperson(lead_id)
        activix_crm_api = ActivixApiWrapper(activity=activity, salesperson=salesperson)

        activix_activity_id = activix_crm_api.create_activity()
        logger.info(f"Activix responded with activity ID: {activix_activity_id}")

        if activix_activity_id: crm_api.update_activity(activity["activity_id"], activix_activity_id)
    except Exception as e:
        error_str = str(e)
        if error_str.startswith('404'):
            logger.error(f"Received 404 error from Activix: {e}")
            return

        logger.exception(f"Failed to post activity {activity['activity_id']} to Activix")
        logger.error("[SUPPORT ALERT] Failed to Send Activity [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nActivityId: {}\nActivityType: {}\nTraceback: {}".format(
            activity["dealer_integration_partner_id"], activity["lead_id"], activity["activity_id"], activity["activity_type"], e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Create activity on Activix."""
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
