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

        sqs_message = True

        # Get oem_partner attribute of dealer
        if body.get("id") and body.get("detail-type"):
            sqs_message = False
            body = body.get("detail", {})
            logger.info("EventBridge message received.")
            crm_api_wrapper = CRMAPIWrapper()
            idp_dealer_id = body.get("idp_dealer_id")
            dealer_db = crm_api_wrapper.get_idp_dealer(idp_dealer_id)
            oem_partner = dealer_db.get("metadata", {}).get("oem_partner", {})
        else:
            # To deprecate, following CRM API deployment
            logger.info("SQS message received.")
            oem_partner = body.get("oem_partner", {})
            return

        lead_id = body.get("lead_id")
        if not lead_id:
            logger.warning("Missing 'lead_id' in record body.")
            return

        oem_class = OemAdfCreation(oem_partner)
        is_vehicle_of_interest = oem_class.create_adf_data(lead_id)

        if not is_vehicle_of_interest:
            # Forward event to ADF Assembler via EventRouting Bus, Remove override partner to allow default routing
            if not sqs_message:
                logger.info(f"Forwarding event to ADF Assembler via EventRouting Bus: {sqs_message}")
                event_bus_client = client('events')
                event_body = {
                    "lead_id": lead_id,
                    "consumer_id": body.get("consumer_id"),
                    "source_application": body.get("source_application"),
                    "idp_dealer_id": body.get("idp_dealer_id"),
                    "event_type": "Lead Created",
                }
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
            else:
                logger.info("Sending message to ADF Assembler via SQS")
                config_key = f"configurations/{ENVIRONMENT}_ADF_ASSEMBLER.json"
                config_data = get_configuration(BUCKET, config_key)
                queue_url = config_data.get("STANDARD_ADF_QUEUE")

                if not queue_url:
                    logger.error("STANDARD_ADF_QUEUE not found in configuration.")
                    return

                send_to_sqs(queue_url, dumps(body))

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
