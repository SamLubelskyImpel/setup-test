import logging
from os import environ
from typing import Any, Dict
from json import loads, dumps
from boto3 import client
from boto3.exceptions import Boto3Error
from shared.oem_adf_creation import OemAdfCreation
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from shared.shared_class import BaseClass as CRMAPIWrapper

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sqs_client = client("sqs")
s3_client = client("s3")

BUCKET = environ.get("INTEGRATIONS_BUCKET", "")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")

processor = BatchProcessor(event_type=EventType.SQS)


def get_configuration(bucket: str, key: str) -> Dict[str, Any]:
    """Retrieve configuration from S3."""
    try:
        logger.info(f"Fetching configuration from S3: Bucket={bucket}, Key={key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return loads(response["Body"].read().decode("utf-8"))
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


def process_record(record: SQSRecord) -> None:
    """Process a single SQS record."""
    logger.info(f"Processing record: {record}")
    try:
        body = record.json_body
        logger.debug(f"Record body: {body}")

        if not (body.get("id") and body.get("detail-type")):
            logger.info("SQS message received. Ignored.")
            return

        logger.info("EventBridge message received.")
        body = body.get("detail", {})

        crm_api_wrapper = CRMAPIWrapper()
        idp_dealer_id = body.get("idp_dealer_id")
        dealer_db = crm_api_wrapper.get_idp_dealer(idp_dealer_id)
        oem_partner = dealer_db.get("metadata", {}).get("oem_partner", {})

        lead_id = body.get("lead_id")
        if not lead_id:
            logger.warning("Missing 'lead_id' in record body.")
            return

        oem_class = OemAdfCreation(oem_partner)
        is_vehicle_of_interest = oem_class.create_adf_data(lead_id)

        if not is_vehicle_of_interest:
            # Forward event to ADF Assembler via EventRouting Bus, Remove override partner to allow default routing
            event_bus_client = client('events')
            event_body = {
                "lead_id": lead_id,
                "consumer_id": body["consumer_id"],
                "source_application": body["source_application"],
                "idp_dealer_id": body["idp_dealer_id"],
                "event_type": "Lead Created",
                "partner_name": body["partner_name"]
            }
            logger.info(f"Forwarding event to ADF Assembler via EventRouting Bus: {event_body}")
            response = event_bus_client.put_events(
                Entries=[
                        {
                            "Source": "JDPAADFAssembler",
                            "DetailType": "JSON",
                            "Detail": dumps(event_body),
                            "EventBusName": f"crm-shared-{ENVIRONMENT}-CrmEventBus"
                        }
                ]
            )
            logger.info(f"Event forwarded to ADF Assembler via EventBus: {response}")

    except Exception as e:
        logger.exception(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any):
    """Lambda function handler."""
    logger.info("Lambda invocation started.")
    try:
        return process_partial_response(
            event=event,
            record_handler=process_record,
            processor=processor,
            context=context,
        )
    except Exception as e:
        logger.error(f"Critical error in batch processing: {e}")
        raise
