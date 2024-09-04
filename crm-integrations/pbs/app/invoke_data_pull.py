import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
from uuid import uuid4
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from api_wrappers import PbsApiWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")

def build_id_list(type: str, leads: list):
   idList = ''
   for lead in leads:
       if type == 'Contact':
           buyerRef = lead.get('BuyerRef', '')
           if buyerRef:
               idList += buyerRef
       if type == 'Vehicle':
            vehicles = lead.get("Vehicles", [])
            if len(vehicles) > 0:
                idList += vehicles[0].get("VehicleRef", '')
       idList += ','
   return idList


def find_value(type: str, dataset: list, id: str):
    for item in dataset:
        if type == "Contact":
            if item.get('ContactId', '') == id:
                return item
        if type == "Vehicle":
            if item.get('VehicleId', '') == id:
                return item
    logger.warning(f"Could not find {type} with ID {id}. Lead may not transform correctly")
    return{}

def fetch_new_leads(start_time: str, crm_dealer_id: str, filtered_lead_types: list):
    """Fetch new leads from PBS CRM."""
    api = PbsApiWrapper()

    # Get inital list of leads
    try:
        inital_leads = api.call_deal_get(start_time, crm_dealer_id).get("Deals")

    except Exception as e:
        logger.error(f"Error occured calling PBS APIs: {e}")
        raise

    logger.info(f"Total initial leads found {len(inital_leads)}")

    # Filter leads
    filtered_leads = filter_leads(inital_leads, filtered_lead_types)
    logger.info(f"Total leads after filtering {len(filtered_leads)}")

    contactIdList = build_id_list('Contact', filtered_leads)
    vehicleIdList = build_id_list('Vehicle', filtered_leads)

    contactList = api.call_contact_get(contactIdList, crm_dealer_id).get('Contacts', [])
    vehicleList = api.call_vehicle_get(vehicleIdList, crm_dealer_id).get('Vehicles', [])

    for lead in filtered_leads:
        contactId = lead.get("BuyerRef", None)
        vehicleId = None
        dealId = lead.get("DealId")

        vehicles = lead.get("Vehicles", [])
        if len(vehicles) > 0:
            vehicleId = vehicles[0].get("VehicleRef", None)

        if contactId:
            try:
                lead["Contact_Info"] = find_value("Contact", contactList, contactId)
            except Exception as e:
                logger.warning(f"Error getting contact info, skipping lead {dealId}")
                continue

        if vehicleId:
            try:
                lead["Vehicle_Info"] = find_value("Vehicle", vehicleList, vehicleId)
            except Exception as e:
                logger.warning(f"Error getting vehicle info, skipping lead {dealId}")
                continue

    return filtered_leads

def filter_leads(leads: list, filtered_lead_types: list):
    """Filter leads by LeadType."""
    filtered_leads = []
    for lead in leads:
        try:
           lead_type = lead.get("LeadType", "")

           if lead_type in filtered_lead_types:
               filtered_leads.append(lead)
    
        except Exception as e:
            logger.error(f"Error parsing Lead type for lead {lead.get('DealId')}. Skipping lead: {e}")
            continue

    return filtered_leads


def save_raw_leads(leads: list, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = '%Y/%m/%d/%H/%M'
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/pbs/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving leads to {s3_key}")
    s3_client.put_object(
        Body=dumps(leads),
        Bucket=BUCKET,
        Key=s3_key,
    )


def record_handler(record: SQSRecord):
    """Invoke PBS data pull."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])
        logger.info(body)

        start_time = body.get("start_time")
        crm_dealer_id = body.get("crm_dealer_id")
        product_dealer_id = body.get("product_dealer_id", "missing_product_dealer")

        filtered_lead_types = body.get("metadata", {}).get("filtered_lead_types", [])

        leads = fetch_new_leads(start_time, crm_dealer_id, filtered_lead_types)
        if not leads:
            logger.info(f"No new leads found for dealer with serial number {crm_dealer_id} for {start_time}")
            return

        save_raw_leads(leads, product_dealer_id)
        logger.info(f"Total leads saved {len(leads)}")

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        logger.error("[SUPPORT ALERT] Failed to Get Leads [CONTENT] ProductDealerId: {}\nDealerId: {}\nStartTime: {}\n\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, start_time, e)
            )
        raise

def lambda_handler(event: Any, context: Any) -> Any:
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
    return