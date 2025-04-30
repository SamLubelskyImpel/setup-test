import logging
from os import environ
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from rds_instance import RDSInstance
import boto3
from json import dumps


logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())
SQS = boto3.client("sqs")

ENRICH_CUSTOMERS_QUEUE = environ.get("ENRICH_CUSTOMERS_QUEUE")
ENVIRONMENT = environ.get("ENVIRONMENT")
CUSTOMER_QUERY_LIMIT = environ.get("CUSTOMER_QUERY_LIMIT", 1)


def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")

    try:
        db_instance = RDSInstance()
        dms_id = record.json_body.get("dms_id")
        customers = db_instance.get_optin_missing_customers(dms_id, limit=CUSTOMER_QUERY_LIMIT)
        logger.info(f"Pulled {len(customers)} customers")
        for customer_id, dms_customer_id in customers:
            queue_event = {
                "dms_id": dms_id,
                "customer_id": customer_id,
                "dms_customer_id": dms_customer_id
            }
            logger.info(f"Sending event {queue_event}")
            SQS.send_message(
                QueueUrl=ENRICH_CUSTOMERS_QUEUE,
                MessageBody=dumps(queue_event)
            )
    except:
        logger.exception(f"Error processing record")
        raise


def pull_customers_handler(event, context):
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
    except:
        logger.exception(f"Error processing batch")
        raise
