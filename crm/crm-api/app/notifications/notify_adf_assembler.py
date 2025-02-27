"""Forward event to ADF Assembler from CRM API."""
import logging
from os import environ
from boto3 import client
from typing import Any, Dict
from json import loads, dumps
from boto3.exceptions import Boto3Error
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

sqs_client = client('sqs')
s3_client = client('s3')
processor = BatchProcessor(event_type=EventType.SQS)

class ADFAssemblerError(Exception):
    """Exception indicating failure to send an event to the ADF Assembler."""
    pass

def load_adf_config() -> Dict[str, str]:
    """Load ADF Assembler configuration from S3 and cache it."""
    try:
        s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_ADF_ASSEMBLER.json"
        config_data = s3_client.get_object(Bucket=BUCKET, Key=s3_key)["Body"].read().decode("utf-8")
        logger.info("Successfully loaded ADF Assembler configuration.")
        return loads(config_data)
    except Boto3Error as e:
        error_message = f"Failed to load ADF configuration: {e}"
        logger.error(error_message)
        send_email_notification(error_message, subject="ADF Configuration Load Failure")
        raise ADFAssemblerError(error_message)

# Load configuration at module load time to avoid multiple S3 calls
adf_config = load_adf_config()

def send_to_adf_assembler(record: Dict[str, Any]) -> None:
    """Send event to ADF Assembler based on event type."""
    try:
        event = record.json_body
        attributes = record.get("attributes", {})
        queue_key = "OEM_PARTNER_ADF_QUEUE" if event.get("oem_partner") else "STANDARD_ADF_QUEUE"

        logger.info(f"adf_config: {adf_config}")
        logger.info(f"queue_key: {queue_key}")

        sqs_url = adf_config.get(queue_key)
        if not sqs_url:
            raise ValueError(f"No SQS URL configured for key '{queue_key}'")

        if queue_key == "OEM_PARTNER_ADF_QUEUE" and 'CreateActivity' in attributes.get('SenderId'):
            logger.info(f"Skipping OEM Partner ADF Assembler for CreateActivity event.")
            return

        logger.info(f"Sending message to {queue_key}\nThis is event: {event}")
        sqs_client.send_message(QueueUrl=sqs_url, MessageBody=dumps(event))
        logger.info("Message successfully sent to ADF Assembler.")
    except (Boto3Error, ValueError) as e:
        error_message = f"Error sending event to ADF Assembler: {e}"
        logger.error(error_message)
        send_email_notification(error_message, subject="ADF Assembler Failure Alert")
        raise ADFAssemblerError(error_message)

def record_handler(record: SQSRecord) -> None:
    """Process each SQS record."""
    logger.info(f"Processing record with message ID: {record.message_id}")
    send_to_adf_assembler(record)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point to process events."""
    logger.info("Starting batch event processing.")
    try:
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        logger.info("Batch event processing complete.")
        return result
    except Exception as e:
        logger.error(f"Critical error processing batch: {e}")
        raise
