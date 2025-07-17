"""Send activity to DealerPeak."""

import logging
from json import loads
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

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord, crm_api: CrmApiWrapper):
    """Create activity on DealerPeak."""
    logger.info(f"Record: {record}")
    try:
        activity = loads(record["body"])
        salesperson = crm_api.get_salesperson(activity["lead_id"])

        dealer_peak_api = DealerpeakApiWrapper(
            activity=activity, salesperson=salesperson
        )

        dealerpeak_task_id = dealer_peak_api.create_activity()
        logger.info(f"Dealerpeak responded with task ID: {dealerpeak_task_id}")

        crm_api.update_activity(activity["activity_id"], dealerpeak_task_id)

    except CRMApiError:
        return
    except Exception as e:
        logger.exception(
            f"Failed to post activity {activity['activity_id']} to Dealerpeak"
        )
        logger.error(
            "[SUPPORT ALERT] Failed to Send Activity [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nActivityId: {}\nActivityType: {}\nTraceback: {}".format(
                activity["dealer_integration_partner_id"],
                activity["lead_id"],
                activity["activity_id"],
                activity["activity_type"],
                e,
            )
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
