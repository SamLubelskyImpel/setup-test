"""
Transform the lead, and upload to CRM Shared Layer
"""

import logging
from datetime import datetime
from json import dumps, loads
from os import environ
from typing import Any
from uuid import uuid4

import boto3
import requests
from requests.auth import HTTPBasicAuth
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def record_handler(record: SQSRecord):
    """
    Transform the lead, and upload to CRM Shared Layer
    """
    logger.info(f"Record: {record}")
    body = loads(record.body)

    # Create customer

    # Create lead



def lambda_handler(event: Any, context: Any) -> Any:
    """
    Transform the lead, and upload to CRM Shared Layer
    """
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
