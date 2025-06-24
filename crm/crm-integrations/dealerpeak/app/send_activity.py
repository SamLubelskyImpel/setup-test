"""Send activity to DealerPeak."""

import logging
from os import environ
from typing import Any

import boto3
from api_wrappers import CRMApiError, CrmApiWrapper, DealerpeakApiWrapper
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord, crm_api: CrmApiWrapper):
    """Create activity on DealerPeak."""
    logger.info(f"Record: {record}")
    activity = {}
    logger.info("HELLO WORLD")
    try:
        body = record.json_body
        details = body.get("detail", {})

        salesperson = crm_api.get_salesperson(details["lead_id"])
        activity = crm_api.get_activity(details["activity_id"])
        dealer = crm_api.get_dealer_by_idp_dealer_id(details["idp_dealer_id"])

        activity["crm_dealer_id"] = dealer["crm_dealer_id"]

        if not activity:
            raise ValueError(f"Activity not found for ID: {details['activity_id']}")

        dealer_peak_api = DealerpeakApiWrapper(
            activity=activity, salesperson=salesperson
        )

        dealerpeak_task_id = dealer_peak_api.create_activity()
        logger.info(f"Dealerpeak responded with task ID: {dealerpeak_task_id}")

        crm_api.update_activity(details["activity_id"], dealerpeak_task_id)

    except CRMApiError:
        return
    except Exception as e:
        logger.exception(
            f"Failed to post activity {details['activity_id']} to Dealerpeak"
        )
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
    """Create activity on DealerPeak."""
    logger.info(f"Event: {event}")
    crm_api = CrmApiWrapper()
    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=lambda record: record_handler(record, crm_api),
            processor=processor,
            context=context,
        )
        return result

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
