from typing import Any, Dict
from os import environ
from logging import getLogger
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from .api_wrappers import CrmApiWrapper, DealersocketAUApiWrapper

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

crm_api = CrmApiWrapper()


def record_handler(record: SQSRecord) -> None:
    """Process a single record to create activity on Dealersocket AU."""
    try:
        activity = record.json_body
        logger.info(f"Processing activity: {activity}")

        salesperson = crm_api.get_salesperson(activity["lead_id"])
        if not salesperson:
            logger.warning(f"No salesperson found for lead ID: {activity['lead_id']}")

        logger.info(f"Salesperson: {salesperson}")

        dealersocket_au_crm_api = DealersocketAUApiWrapper(
            activity=activity, salesperson=salesperson
        )
        dealersocket_au_response = dealersocket_au_crm_api.create_activity()
        activity_id = dealersocket_au_response.get("ActivityID")
        error_code = dealersocket_au_response.get("ErrorCode")

        if error_code:
            if error_code == "INTERNAL_ERROR":
                raise RuntimeError("Internal error occurred in Dealersocket AU API")
            logger.error(
                f"Error from Dealersocket AU: {error_code} - Response: {dealersocket_au_response}"
            )
            return

        if activity_id:
            logger.info(f"Activity created with ID: {activity_id}")
            crm_api.update_activity(activity["activity_id"], dealersocket_au_response)
        elif activity["activity_type"] == "note":
            logger.info("No activity ID expected when creating worknotes.")
        else:
            logger.warning(
                f"No Activity ID received. Dealersocket response: {dealersocket_au_response}"
            )
    except Exception as e:
        logger.error(
            f"Failed to post activity | Activity ID: {activity.get('activity_id')}, "
            f"Lead ID: {activity.get('lead_id')} | Error: {str(e)}"
        )
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Any:
    """Batch processor for handling SQS events."""
    logger.info(f"Lambda invoked with event: {event}")
    processor = BatchProcessor(event_type=EventType.SQS)
    try:
        return process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
