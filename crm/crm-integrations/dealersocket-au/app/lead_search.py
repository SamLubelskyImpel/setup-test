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

from pentana_client import PentanaClient

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
        raise Exception(f"Error sending message to queue: {queue_url}: {e}")


def match_carsales_filter(event, carsales_data) -> bool:
    """
    Check if the event data matches the Carsales stock number or, if not, the make and model.

    Stock number is extracted from the Identification array in the Carsales data.
    """
    item = carsales_data.get("Item", {})

    # Extract stock number from the Identification array.
    stock_number = None
    identifications = item.get("Identification", [])
    if isinstance(identifications, list):
        for item in identifications:
            if item.get("Type", "").lower() == "stocknumber":
                stock_number = item.get("Value")
                break
    elif isinstance(identifications, dict):
        if identifications.get("Type", "").lower() == "stocknumber":
            stock_number = identifications.get("Value")

    return (event.get("stockNumber") == stock_number or
            (event.get("make") == item.get("Make") and
             event.get("model") == item.get("Model")))


def process_event_response(event_response: dict, carsales_json_data: dict):
    """
    Check if any event matches the carsales data
    """
    events = event_response.get("events")
    if not events:
        raise Exception("No events received from Dealersocket Events API")

    for event in events:
        if match_carsales_filter(event, carsales_json_data):
            matched_event = event
            break
    else:
        raise Exception("No matching events found in Dealersocket Events API response")

    logger.info(f"Matched Event: {matched_event}")
    return matched_event


def match_customer_record(prospect, customer_records) -> list:
    """
    Given the prospect data from Carsales and a list of customer records,
    return a list of PartyIDs that match based on first name, last name, and either
    email, mobile phone, or home phone.
    """
    matches = []
    prospect_first = prospect.get("FirstName", "").lower()
    prospect_last = prospect.get("LastName", "").lower()
    prospect_email = prospect.get("Email", "").lower()
    prospect_mobile = prospect.get("MobilePhone", "").lower()
    prospect_home = prospect.get("HomePhone", "").lower()

    if not isinstance(customer_records, list):
        customer_records = [customer_records]

    for record in customer_records:
        detail = record.get("CustomerInformationDetail", {})
        party = detail.get("CustomerParty", {})
        specified = party.get("SpecifiedPerson", {})
        record_first = specified.get("GivenName", "").lower()
        record_last = specified.get("FamilyName", "").lower()

        record_email = None
        if "URICommunication" in specified:
            uri = specified.get("URICommunication")
            if isinstance(uri, list):
                for u in uri:
                    if u.get("ChannelCode", "").lower() == "email address":
                        record_email = u.get("URIID", "").lower()
                        break
            else:
                record_email = uri.get("URIID", "").lower()

        # Extract mobile and home numbers from record
        record_mobile = None
        record_home = None
        if "TelephoneCommunication" in specified:
            tc = specified.get("TelephoneCommunication")
            if isinstance(tc, list):
                for number in tc:
                    use_code = number.get("UseCode", "").lower()
                    if use_code == "mobile" and not record_mobile:
                        record_mobile = number.get("CompleteNumber", "").lower()
                    elif use_code == "home" and not record_home:
                        record_home = number.get("CompleteNumber", "").lower()
            else:
                record_mobile = tc.get("CompleteNumber", "").lower()

        # Match on first and last names first.
        if record_first == prospect_first and record_last == prospect_last:
            if prospect_email and record_email and prospect_email == record_email:
                logger.info("Matched record with PartyID: %s via email.", party.get("PartyID"))
                matches.append(party.get("PartyID"))
            elif prospect_mobile and record_mobile and prospect_mobile == record_mobile:
                logger.info("Matched record with PartyID: %s via mobile phone.", party.get("PartyID"))
                matches.append(party.get("PartyID"))
            elif prospect_home and record_home and prospect_home == record_home:
                logger.info("Matched record with PartyID: %s via home phone.", party.get("PartyID"))
                matches.append(party.get("PartyID"))
    return matches



def format_prospect(new_prospect: dict) -> dict:
    """Convert the new Carsales prospect structure into a legacy format with separate fields."""
    formatted = {}
    # Split the "Name" field into first and last names.
    full_name = new_prospect.get("Name", "")
    parts = full_name.split()
    formatted["FirstName"] = parts[0] if parts else ""
    formatted["LastName"] = " ".join(parts[1:]) if len(parts) > 1 else ""

    formatted["Email"] = new_prospect.get("Email", "")

    mobile = ""
    home = ""
    for phone in new_prospect.get("PhoneNumbers", []):
        phone_type = phone.get("Type", "").lower()
        if phone_type == "mobile" and not mobile:
            mobile = phone.get("Number", "")
        elif phone_type == "home" and not home:
            home = phone.get("Number", "")

    formatted["MobilePhone"] = mobile
    formatted["HomePhone"] = home

    return formatted


def record_handler(record: SQSRecord):
    """
    Search for leads in DealerSocket AU. If found, merge with Carsales data
    and send to LeadTransformationQueue.
    """
    logger.info(f"Record: {record}")
    try:
        # Load Carsales raw object data from S3
        message = loads(record["body"])
        bucket = message["detail"]["bucket"]["name"]
        key = message["detail"]["object"]["key"]

        # Extract crm_dealer.product_dealer_id from the key
        key_parts = key.split('/')
        product_dealer_id = key_parts[2]

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        carsales_json_data = loads(content)
        logger.info(f"Processing carsales data: {carsales_json_data}")

        # Extract prospect data
        raw_prospect = carsales_json_data.get("Prospect")
        if not raw_prospect:
            raise ValueError("Missing Prospect data in raw Carsales data")

        prospect = format_prospect(raw_prospect)

        if not prospect:
            raise ValueError("Missing Prospect data in raw Carsales data")

        # Extract dealer_id
        dealer_id = carsales_json_data.get("crm_dealer_id")
        if not dealer_id:
            raise ValueError("Missing crm_dealer_id in raw Carsales data")

        # Initialize PentanaClient client
        pentana_client = PentanaClient()

        # Query customer based on prospect data (aggregating results from multiple API calls)
        customer_records = pentana_client.query_entity(DEALERSOCKET_VENDOR, dealer_id, prospect)
        logger.info(f"Unique customer records: {customer_records}")

        # Filter the aggregated customer records using matching function
        matched_entity_ids = match_customer_record(prospect, customer_records)

        if not matched_entity_ids:
            raise Exception("No matching customer found in Pentana response")

        matched_entity_id = None
        processed_event = None
        for party_id in matched_entity_ids:
            logger.info(f"party id is: {party_id}")
            try:
                event_response = pentana_client.query_event(DEALERSOCKET_VENDOR, dealer_id, party_id)
                # Use process_event_response to filter events by matching vehicle details.
                processed_event = process_event_response(event_response, carsales_json_data)
                if processed_event:
                    matched_entity_id = party_id
                    break
            except Exception as e:
                logger.error(f"Error querying event for PartyID {party_id}: {e}")
        if not matched_entity_id:
            raise Exception("No event found for any matched customer")

        # Retrieve the matched entity record from the aggregated customer records.
        matched_entity = next(
            (
                rec for rec in customer_records
                if rec.get("CustomerInformationDetail", {})
                    .get("CustomerParty", {})
                    .get("PartyID") == matched_entity_id
            ),
            None
        )

        logger.info(f"Matched Entity ID: {matched_entity_id}")
        logger.info(f"Pentana Lead API event Response: {processed_event}")

        # Merge response with Carsales data
        merged_data = {
            "carsales_data": carsales_json_data,
            "entity_response": matched_entity,
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