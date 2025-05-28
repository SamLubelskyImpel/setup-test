import logging
from os import environ
from typing import Any
from json import dumps
import boto3
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

DA_EVENT_FORWARDER_LAMBDA_ARN = environ.get("DA_EVENT_FORWARDER_LAMBDA_ARN")

lambda_client = boto3.client("lambda")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def record_handler(record: SQSRecord):
    """
    Process a single new lead event SQS record.
    """

    body = record.json_body
    details = body.get("detail", {})

    if "lead_id" not in details or not isinstance(details.get("lead_id"), int):
        logger.error(f"[new_lead_events_handler] lead_id not found in event body: \n {details}")
        return

    lambda_client.invoke(
        FunctionName=DA_EVENT_FORWARDER_LAMBDA_ARN,
        InvocationType="Event",
        Payload=dumps(details).encode("UTF-8"),
    )

    logger.info(f"[new_lead_events_handler] lead_id: {details.get('lead_id')}")


def lambda_handler(event: Any, context: Any) -> Any:
    """Lambda function to catch new lead events from CrmEventBus
    and send them to DA Event Forwarder Lambda asynchronously."""

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
