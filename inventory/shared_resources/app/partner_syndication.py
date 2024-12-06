import logging
from json import loads
from os import environ
from typing import Any
from io import StringIO

import boto3
import pandas as pd
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from partner_uploader import get_partner_uploaders

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        # Get the icc file key
        body = loads(record['body'])
        bucket = body["Records"][0]["s3"]["bucket"]["name"]
        key = body["Records"][0]["s3"]["object"]["key"]
        provider_dealer_id = key.split('/')[-1].split('.')[0]

        logger.info(f"Uploaded for Provider dealer id: {provider_dealer_id}")

        # Download the icc file
        response = s3_client.get_object(Bucket=bucket, Key=key)

        # Convert file to Pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')
        icc_formatted_inventory = pd.read_csv(StringIO(csv_content))

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
