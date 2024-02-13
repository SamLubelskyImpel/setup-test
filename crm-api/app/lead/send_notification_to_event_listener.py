import logging
import os
import json
import boto3
from os import environ
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    SqsFifoPartialProcessor,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")

    message_body = record['body']
    message = json.loads(message_body)
    
    lead_id = message['lead_id']
    
    logger.info(f"Processing lead_id: {lead_id}")
    


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw reyrey lead update data to the unified format."""
    logger.info(f"Event: {event}")

    try:
        processor = SqsFifoPartialProcessor()
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
