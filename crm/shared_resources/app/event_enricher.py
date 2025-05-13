
import logging
from os import environ
import boto3
from utils import call_crm_api
from json import dumps, loads

from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
CRM_API_URL = environ.get("CRM_API_URL")
CRM_EVENT_BUS_NAME = environ.get("CRM_EVENT_BUS_NAME")

secret_client = boto3.client("secretsmanager")
logger = logging.getLogger()


def record_handler(record: SQSRecord):
    """
    Process a single EventEnricher SQS record.
    """
    logger.info(f"Record: {record}")

    try:
        body = loads(record["body"])
        idp_dealer_id = body.get("Detail", {}).get("idp_dealer_id")
        dealer = call_crm_api(f"{CRM_API_URL}dealers/idp/{idp_dealer_id}")

        if not dealer:
            logger.error(f"Dealer not found for idp_dealer_id: {idp_dealer_id}")
            return {
                "statusCode": 404,
                "body": f"Dealer not found for idp_dealer_id: {idp_dealer_id}"
            }

        metadata = dealer.get("metadata") or {}
        oem_partner = metadata.get("oem_partner") or {}
        oem_partner_name = oem_partner.get("name")

        body["Detail"]["partner_name"] = dealer['integration_partner_name']
        body["Detail"]["override_partner"] = f"{oem_partner_name}" if oem_partner_name else None

        logger.info(f"Event to be sent to EventBus: {body}")

        event_bus_client = boto3.client('events')
        response = event_bus_client.put_events(
            Entries=[
                    {
                        "Source": "EventEnricher",
                        "DetailType": body["DetailType"],
                        "Detail": dumps(body["Detail"]),
                        "EventBusName": body["EventBusName"]
                    }
            ]
        )
        logger.info(f"Event sent to EventBus: {response}")

        return {
            "statusCode": 200,
            "body": body
        }

    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return {
            "statusCode": 500,
            "body": f"Error processing event: {e}"
        }


def lambda_handler(event, context):
    """
    This function is triggered by a CRM API event.
    It processes the event, adds additional information
    and sends it to an CrmEventBus.
    """
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
