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
        config_data = s3_client.get_object(
            Bucket=BUCKET, Key=f"configurations/{ENVIRONMENT}_ADF_ASSEMBLER.json"
        )["Body"].read().decode("utf-8")
        logger.info("Successfully loaded ADF Assembler configuration.")
        return loads(config_data)
    except Boto3Error as e:
        error_message = f"Failed to load ADF configuration: {e}"
        logger.error(error_message)
        send_email_notification(error_message, subject="ADF Configuration Load Failure")
        raise ADFAssemblerError(error_message)

# Load configuration at module load time to avoid multiple S3 calls
adf_config = load_adf_config()

def send_to_adf_assembler(event: Dict[str, Any]) -> None:
    """Send event to the appropriate ADF Assembler queue."""
    try:
        queue_url = None
        oem_partner = event.get("oem_partner", {}).get("name", "").upper()

        # Check if OEM partner exists in OEM_PARTNER_QUEUES (new structure)
        if oem_partner:
            queue_details = adf_config.get("OEM_PARTNER_QUEUES", {}).get(oem_partner)
            if queue_details:
                queue_url = queue_details.get("queue_url")
                logger.info(f"OEM Partner '{oem_partner}' matched with queue: {queue_url}")

        # If no queue was found in OEM_PARTNER_QUEUES, use the fallback OEM_PARTNER_ADF_QUEUE
        if not queue_url:
            queue_url = adf_config.get("OEM_PARTNER_ADF_QUEUE")

        # If still no queue is found, fallback to the STANDARD_ADF_QUEUE
        if not queue_url:
            queue_url = adf_config.get("STANDARD_ADF_QUEUE")

        # If no queue is configured at all, raise an error
        if not queue_url:
            raise ValueError("No SQS URL configured for the event.")

        logger.info(f"Sending message to queue URL: {queue_url}")
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=dumps(event))
        logger.info("Message successfully sent to ADF Assembler.")

    except (Boto3Error, ValueError) as e:
        error_message = f"Error sending event to ADF Assembler: {e}"
        logger.error(error_message)
        send_email_notification(error_message, subject="ADF Assembler Failure Alert")
        raise ADFAssemblerError(error_message)



def record_handler(record: SQSRecord) -> None:
    """Process each SQS record."""
    logger.info(f"Processing record with message ID: {record.message_id}")
    send_to_adf_assembler(record.json_body)

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


{
  "STANDARD_ADF_QUEUE": "https://sqs.us-east-1.amazonaws.com/143813444726/adf-assembler-test-AdfAssemblerQueue",
  "OEM_PARTNER_ADF_QUEUE": "https://sqs.us-east-1.amazonaws.com/143813444726/honda-acura-adf-assembler-test-HondaAcuraAdfAssemblerQueue",
  "OEM_PARTNER_QUEUES": {
    "STELLANTIS": {
      "partners": [
        "STELLANTIS"
      ],
      "queue_url": "https://sqs.us-east-1.amazonaws.com/143813444726/shift-digital-test-PostLeadQueue"
    }
  }
}