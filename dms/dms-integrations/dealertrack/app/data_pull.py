from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
import logging
import os
from data_pull_manager import DataPullManager


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

IS_PROD = os.environ.get("ENVIRONMENT") == "prod"
INTEGRATIONS_BUCKET = os.environ.get("INTEGRATIONS_BUCKET")


def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")

    try:
        body: dict = record.json_body
        dms_id = body.get("dms_id")
        date = body.get("date")
        resource = body.get("resource")

        logger.info(f"Starting data pull for {dms_id} {resource} {date}")
        manager = DataPullManager(dms_id, date, resource)
        manager.start()
    except:
        logger.exception(f"Error processing record")
        raise


def lambda_handler(event, context):
    """Lambda function entry point for processing SQS messages."""
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
        logger.error(f"Error processing records: {e}")
        raise
