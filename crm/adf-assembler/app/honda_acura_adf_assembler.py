import logging
from os import environ
from typing import Any
from json import loads
from boto3 import client
from boto3.exceptions import Boto3Error
from shared.oem_adf_creation import OemAdfCreation
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sqs_client = client("sqs")
s3_client = client('s3')

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")

processor = BatchProcessor(event_type=EventType.SQS)

def record_handler(record: SQSRecord) -> None:
    """Process each record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])
        logger.info(f"Processing record body: {body}")

        oem_partner = body.get("oem_partner", {})
        oem_class = OemAdfCreation(oem_partner)
        is_vehicle_of_interest = oem_class.create_adf_data(body.get("lead_id"))

        if not is_vehicle_of_interest:
            try:
                config_data = s3_client.get_object(
                    Bucket=BUCKET, Key=f"configurations/{ENVIRONMENT}_ADF_ASSEMBLER.json"
                )["Body"].read().decode("utf-8")
                queue_url = loads(config_data)['STANDARD_ADF_QUEUE']
                sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=record["body"],
                )
                logger.info("Successfully sent message to ADF queue.")
            except Boto3Error as e:
                logger.error(f"Failed to send message to ADF queue: {e}")
    except Exception as e:
        logger.exception(f"Error processing SQS record.\n{e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Process and syndicate event to ADF Assembler."""
    logger.info(f"Event received: {event}")
    try:
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
