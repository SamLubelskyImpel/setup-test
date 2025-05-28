"""Send activity to Big Motoring World."""
import logging
from typing import Any
from os import environ
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from api_wrappers import BigMotoringWorldApiWrapper, CrmApiWrapper, CRMApiError

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
crm_api = CrmApiWrapper()

def record_handler(record: SQSRecord):
    """Send activity to Big Motoring World."""
    logger.info(f"Record: {record}")
    activity = {}

    try:
        body = record.json_body
        details = body.get("detail", {})

        activity = crm_api.get_activity(details["activity_id"])
        dealer = crm_api.get_dealer_by_idp_dealer_id(details["idp_dealer_id"])

        if not activity:
            raise ValueError(f"Activity not found for ID: {details['activity_id']}")

        logger.info(f"Activity: {activity}")
        bmw_api = BigMotoringWorldApiWrapper(activity=activity)

        bmw_response = bmw_api.create_activity()
        logger.info(f"Big Motoring World responded with status: {bmw_response}")
    except CRMApiError as e:
        logger.error(f"CRM API Error: {e}")
        return
    except Exception as e:
        logger.exception(f"Failed to post activity {details['activity_id']} to Big Motoring World")
        logger.error(f"[SUPPORT ALERT] Failed to Send Activity to Big Motoring World [CONTENT]"
                     f"IdpDealerId: {activity.get('idp_dealer_id', '')}\n"
                     f"LeadId: {details['lead_id']}\n"
                     f"ActivityId: {details['activity_id']}\n"
                     f"ActivityType: {activity.get('activity_type', '')}\n"
                     f"Traceback: {e}"
        )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Send activity to Big Motoring World."""
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
