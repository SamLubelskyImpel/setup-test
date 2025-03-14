import boto3
from json import loads, dumps
from os import environ
import logging
from typing import Any
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
TRANSFORM_NEW_LEAD_QUEUE = environ.get("TRANSFORM_NEW_LEAD_QUEUE")

sqs = boto3.client('sqs')
s3_client = boto3.client('s3')


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        message = loads(record.body)
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]
        product_dealer_id = key.split('/')[2]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        json_data = loads(content)
        logger.info(f"Raw data: {json_data}")

        crm_dealer_id = json_data["dealerID"]
        crm_consumer_id = json_data["personApiID"]
        crm_lead_id = json_data["id"]

        message_body = dumps({
            "bucket": bucket,
            "key": key,
            "product_dealer_id": product_dealer_id
        })

        sqs.send_message(
            QueueUrl=TRANSFORM_NEW_LEAD_QUEUE,
            MessageBody=message_body,
            MessageGroupId=crm_consumer_id,
            MessageDeduplicationId=crm_lead_id
        )

    except Exception as e:
        logger.error(f"Error deduplicating momentum record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Deduplicate New Lead [CONTENT] ProductDealerId: {}\nDealerId: {}\nLeadId: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, crm_lead_id, e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Deduplicate raw momentum leads."""
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
