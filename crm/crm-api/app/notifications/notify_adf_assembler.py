"""Forward event to ADF Assembler from CRM API."""
import boto3
import logging
from os import environ
from typing import Any
from json import loads, dumps
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from utils import send_email_notification

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

class ADFAssemblerError(Exception):
    """An exception indicating a failure to send an event to the ADF Assembler."""
    pass


def send_to_adf_assembler(event: Any) -> None:
    """Send event to ADF Assembler."""
    try:
        s3_object = loads(
            s3_client.get_object(
                Bucket=BUCKET, Key=f"configurations/{ENVIRONMENT}_ADF_ASSEMBLER.json"
            )["Body"]
            .read()
            .decode("utf-8")
        )
        sqs_url = s3_object.get("STANDARD_ADF_QUEUE")
        if event.get("oem_partner"):
            sqs_url = s3_object.get("OEM_PARTNER_ADF_QUEUE")

        sqs_client.send_message(
            QueueUrl=sqs_url,
            MessageBody=dumps(event)
        )
    except Exception as e:
        message = f"Error sending the event to ADF Assembler: {e}"
        logger.error(message)
        send_email_notification(message, subject="ADF Assembler Failure Alert")
        raise ADFAssemblerError(message)


def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        message_body = record['body']
        event = loads(message_body)

        logger.info(f"Processing event: {event}")
        send_to_adf_assembler(event)
    except ADFAssemblerError:
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Process and syndicate event to ADF Assembler."""
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
