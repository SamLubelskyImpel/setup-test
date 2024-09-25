"""Send activity to Eskimo."""
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
from api_wrappers import EskimoApiWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord):
    """Send activity to Eskimo."""
    logger.info(f"Record: {record}")
    try:
        activity = loads(record['body'])
        logger.info(f"Activity: {activity}")
        eskimo_api = EskimoApiWrapper(activity=activity)

        eskimo_response = eskimo_api.create_activity()
        
        logger.info(f"Eskimo responded with status: {eskimo_response}")
    except Exception as e:
        logger.exception(f"Failed to post activity {activity['activity_id']} to Eskimo")
        logger.error("[SUPPORT ALERT] Failed to Send Activity to Eskimo [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nActivityId: {}\nActivityType: {}\nTraceback: {}".format(
            activity["dealer_integration_partner_id"], activity["lead_id"], activity["activity_id"], activity["activity_type"], e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Send activity to Eskimo."""
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