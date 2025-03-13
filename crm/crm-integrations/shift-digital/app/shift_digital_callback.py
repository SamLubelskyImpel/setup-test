import logging
import json
from typing import Any, Dict
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from api_wrappers import ShiftDigitalAPIWrapper

# Logging Configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Batch Processor
processor = BatchProcessor(event_type=EventType.SQS)


def process_callback(record: SQSRecord) -> None:
    """Process each SQS record for Shift Digital callback."""
    logger.info(f"Processing record: {record}")

    try:
        message_body = json.loads(record.body)
        logger.info(f" Processing body: {message_body}")
        shift_digital_lead_id = message_body["shift_digital_lead_id"]
        lead_id = message_body["lead_id"]

        if not shift_digital_lead_id:
            logger.error("Missing required shift_digital_lead_id")
            raise ValueError("Missing required fields: shift_digital_lead_id")

        shift_digital_api = ShiftDigitalAPIWrapper()
        shift_digital_api.process_callback(shift_digital_lead_id, lead_id)

    except Exception as e:
        logger.error(f"Error processing Shift Digital callback: {e}")
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda function handler."""
    logger.info("Shift Digital Callback Lambda invoked.")
    try:
        return process_partial_response(
            event=event,
            record_handler=process_callback,
            processor=processor,
            context=context,
        )
    except Exception as e:
        logger.error(f"Critical error in callback processing: {e}")
        raise