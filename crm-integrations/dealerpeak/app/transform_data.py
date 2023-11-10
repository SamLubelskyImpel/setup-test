"""Transform raw dealerpeak data to the unified format"""

import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord) -> Any:
    """Process individual SQS record."""
    logger.info(f"Record: {record}")

    try:
        body = loads(record['body'])
        logger.info(f"Data: {body}")
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw dealerpeak data to the unified format."""
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
