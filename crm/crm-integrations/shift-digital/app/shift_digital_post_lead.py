import logging
import json
from os import environ
from typing import Any, Dict
import boto3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from api_wrappers import ShiftDigitalAPIWrapper, CRMAPIWrapper
from boto3.exceptions import Boto3Error

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
CALLBACK_QUEUE_URL = environ.get("SHIFT_DIGITAL_CALLBACK_QUEUE")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

processor = BatchProcessor(event_type=EventType.SQS)


class MissingConfigurationError(Exception):
    """Exception raised when required configuration is missing."""
    pass


def get_configuration(bucket: str, key: str) -> Dict[str, Any]:
    """
    Retrieve configuration from S3.
    :param bucket: S3 bucket name.
    :param key: S3 object key.
    :return: Configuration data as a dictionary.
    """
    try:
        logger.info(f"Fetching configuration from S3: Bucket={bucket}, Key={key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Boto3Error as e:
        logger.error(f"Error fetching configuration from S3: {e}")
        raise


def send_to_sqs(queue_url: str, message_body: str) -> None:
    """Send a message to SQS."""
    try:
        logger.info(f"Sending message to SQS: QueueUrl={queue_url}")
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
        logger.info("Message successfully sent to SQS.")
    except Boto3Error as e:
        logger.error(f"Failed to send message to SQS: {e}")
        raise


def record_handler(record: SQSRecord) -> None:
    """Processes incoming lead."""
    logger.info(f"Processing record: {record}")
    body = record.json_body

    if not (body.get("id") and body.get("detail-type")):
        logger.info("SQS message received. Ignored.")
        return

    logger.info("EventBridge message received.")
    body = body.get("detail", {})
    crm_api_wrapper = CRMAPIWrapper()
    dealer_db = crm_api_wrapper.get_idp_dealer(body["idp_dealer_id"])
    oem_partner = dealer_db.get("metadata", {}).get("oem_partner", {})

    lead_id = body.get("lead_id")
    dealer_code = oem_partner.get("dealer_code")

    if not lead_id or not dealer_code:
        logger.error("Missing required lead_id or dealer_code in request")
        raise ValueError("Missing required fields: lead_id or dealer_code")

    logger.info(f"Processing lead ID {lead_id} for Shift Digital (Dealer Code: {dealer_code})...")

    shift_api = ShiftDigitalAPIWrapper(oem_partner)

    try:
        shift_digital_lead_id, is_vehicle_of_interest = shift_api.submit_lead(lead_id, dealer_code)

        if is_vehicle_of_interest:
            # Only process the callback when it's a Vehicle of Interest
            if not shift_digital_lead_id:
                logger.error("Shift Digital API did not return a lead ID for a Vehicle of Interest.")
                raise ValueError("Shift Digital API did not return a lead ID.")

            logger.info(f"Successfully posted lead {lead_id} to Shift Digital. Shift Digital Lead ID: {shift_digital_lead_id}")

            sqs_payload = {
                "shift_digital_lead_id": shift_digital_lead_id,
                "lead_id": lead_id,
            }
            sqs_client.send_message(
                QueueUrl=CALLBACK_QUEUE_URL,
                MessageBody=json.dumps(sqs_payload),
            )
            logger.info(f"Queued Shift Digital Lead ID {shift_digital_lead_id} for callback processing.")

        else:
            event_bus_client = boto3.client('events')
            event_body = {
                "lead_id": lead_id,
                "consumer_id": body.get("consumer_id"),
                "source_application": body.get("source_application"),
                "idp_dealer_id": body.get("idp_dealer_id"),
                "event_type": "Lead Created",
            }
            logger.info(f"Sending message to ADF Assembler via EventRouting Bus: {event_body}")
            response = event_bus_client.put_events(
                Entries=[
                        {
                            "Source": "JDPAADFAssembler",
                            "DetailType": "JSON",
                            "Detail": json.dumps(event_body),
                            "EventBusName": f"crm-shared-{ENVIRONMENT}-CrmEventBus"
                        }
                ]
            )
            logger.info(f"Event forwarded to ADF Assembler via EventBus: {response}")

    except Exception as e:
        logger.exception(f"Error posting lead to Shift Digital: {e}")
        raise


def lambda_handler(event: Any, context: Any):
    """Lambda function handler."""
    logger.info("Lambda invocation started.")
    try:
        return process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
    except Exception as e:
        logger.error(f"Critical error in batch processing: {e}")
        raise
