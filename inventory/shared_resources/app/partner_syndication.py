import boto3
from json import loads
from os import environ
from typing import Any
import logging
import pandas as pd
from datetime import datetime
import tempfile
from io import BytesIO
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from partner_uploader import get_partner_uploaders

ENVIRONMENT = environ["ENVIRONMENT"]
INVENTORY_BUCKET = environ["INVENTORY_BUCKET"]
MERCH_SFTP_KEY = environ["MERCH_SFTP_KEY"]
SALESAI_SFTP_KEY = environ["SALESAI_SFTP_KEY"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")

def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        # Get the icc file key

        # Download the icc file

        # Upload to the partners
        for uploader in get_partner_uploaders(provider_dealer_id, icc_formatted_inventory):
                uploader.upload()
    except Exception:
        logger.exception("Error processing record")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Convert to ICC format and syndication to product SFTPs."""
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