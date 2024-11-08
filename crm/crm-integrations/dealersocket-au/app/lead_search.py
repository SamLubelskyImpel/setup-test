"""
Search for leads in DealerSocket AU. If found, merge with Carsales data
and send to IngestLeadQueue.
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
INGEST_LEAD_QUEUE_URL = environ.get("INGEST_LEAD_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def send_message_to_queue(queue_url: str, message: dict):
    """
    Send message to queue
    """
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(message)
        )
        logger.info(f"Message sent: {response}")
        return response
    except Exception as e:
        logger.error(f"Error sending message to queue: {queue_url}")
        raise e


def record_handler(record: SQSRecord):
    """
    Search for leads in DealerSocket AU. If found, merge with Carsales data
    and send to IngestLeadQueue.
    """
    logger.info(f"Record: {record}")
    try:
        # Load carsales raw object data from s3
        message = loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        json_data = loads(content)
        logger.info(f"Raw data: {json_data}")



        # Send message to IngestLeadQueue
        message = loads(record.body)
        send_message_to_queue(queue_url=INGEST_LEAD_QUEUE_URL, message=message)
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise e


def lambda_handler(event: Any, context: Any) -> Any:
    """
    Search for leads in DealerSocket AU. If found, merge with Carsales data
    and send to IngestLeadQueue.
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
