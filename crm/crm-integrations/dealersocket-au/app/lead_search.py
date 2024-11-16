"""
Search for leads in DealerSocket AU. If found, merge with Carsales data
and send to IngestLeadQueue.
"""

import logging
from json import dumps, loads
from os import environ
from typing import Any

import boto3
import xmltodict
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from dealer_socket_client import DealerSocketClient

LEAD_TRANSFORMATION_QUEUE_URL = environ.get("LEAD_TRANSFORMATION_QUEUE_URL")
DEALERSOCKET_VENDOR = environ.get("DEALERSOCKET_VENDOR")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def send_message_to_queue(queue_url: str, message: dict):
    """
    Send message to queue
    """
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(message)
        )
        logger.info(f"Message sent: {response}")
        return response
    except Exception as e:
        logger.error(f"Error sending message to queue: {queue_url}")
        raise


def match_carsales_filter(event, carsales_data) -> bool:
    """
    Check if the event data matches the carsales stockNumber or make, model, and year.
    """
    return (event.get("stockNumber") == carsales_data.get("Item").get("StockNumber") or
            (event.get("make") == carsales_data.get("Item").get("Make") and
             event.get("model") == carsales_data.get("Item").get("Model") and
             event.get("year") == carsales_data.get("Item").get("Year")))


def process_event_response(event_response: dict, carsales_json_data: dict):
    """
    Check if any event matches the carsales data
    """
    try:
        events = event_response.get("events")
        if events:
            for event in events:
                if match_carsales_filter(event, carsales_json_data):
                    return event
        else:
            logger.info("No events received from Dealersocket Events API")
    except Exception as e:
        logger.error("Error processing event")
        raise


def record_handler(record: SQSRecord):
    """
    Search for leads in DealerSocket AU. If found, merge with Carsales data
    and send to LeadTransformationQueue.
    """
    logger.info(f"Record: {record}")
    try:
        # Load carsales raw object data from s3
        message = loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]

        # Extract crm_dealer.product_dealer_id from the key
        key_parts = key.split('/')
        product_dealer_id = key_parts[2]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        carsales_json_data = loads(content)
        logger.info(f"Raw data: {carsales_json_data}")

        # Extract prospect data
        prospect = carsales_json_data.get("Prospect")

        if not prospect:
            logger.error("Missing Prospect data in raw carsales data")
            raise ValueError("Missing Prospect data in raw carsales data")

        # Query event based on dealer_id, entity_id, and event
        dealer_id = carsales_json_data.get("crm_dealer_id")

        if not dealer_id:
            logger.error("Missing crm_dealer_id in raw carsales data")
            raise ValueError("Missing crm_dealer_id in raw carsales data")

        # Initialize DealerSocket client
        dealersocket_client = DealerSocketClient()

        # Query customer based on prospect data
        entity_xml_response = dealersocket_client.query_entity(DEALERSOCKET_VENDOR, dealer_id, prospect)
        logger.info(f"Dealersocket Entity API response: {entity_xml_response}")

        entity_response = xmltodict.parse(entity_xml_response)

        entity_id = entity_response.get("ShowCustomerInformation", {}) \
            .get("ShowCustomerInformationDataArea", {}) \
            .get("CustomerInformation", {}) \
            .get("CustomerInformationDetail", {}) \
            .get("CustomerParty", {}) \
            .get("PartyID")

        if not entity_id:
            logger.error("Missing entity_id in entity response")
            raise ValueError("Missing entity_id in entity response")

        event_response = dealersocket_client.query_event(
            DEALERSOCKET_VENDOR,
            dealer_id,
            entity_id
        )

        # Check if any event matches the carsales data
        if not event_response:
            logger.info("No event returned by DealerSocket AU")
            return
        logger.info(f"Dealersocket Event API Response: {event_response}")

        processed_event = process_event_response(event_response, carsales_json_data)

        if not processed_event:
            logger.info("No event in the Dealersocket Event API response matched the Carsales Data")
            return

        # Merge response with Carsales data
        merged_data = {
            "carsales_data": carsales_json_data,
            "entity_response": entity_response,
            "event_response": processed_event,
            "product_dealer_id": product_dealer_id
        }

        # Send message to IngestLeadQueue
        send_message_to_queue(LEAD_TRANSFORMATION_QUEUE_URL, merged_data)
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """
    Search for leads in DealerSocket AU. If found, merge with Carsales data
    and send to IngestLeadQueue.
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
