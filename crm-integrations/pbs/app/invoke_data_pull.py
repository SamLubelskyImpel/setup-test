import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
import requests
from requests.auth import HTTPBasicAuth
from uuid import uuid4
from datetime import datetime
import pandas as pd
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from pbs_api_wrapper import APIWrapper

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")

# def get_secrets():
#     """Get PBS API secrets."""
#     secret = secret_client.get_secret_value(
#         SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
#     )
#     secret = loads(secret["SecretString"])[str(SECRET_KEY)]
#     secret_data = loads(secret)

#     return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]


def fetch_new_leads(start_time: str, crm_dealer_id: str, filtered_lead_types: list):
    """Fetch new leads from PBS CRM."""
    api = APIWrapper()

    # Get inital list of leads
    try:
        inital_leads = api.call_deal_get(start_time, crm_dealer_id).get("Deals")

    except Exception as e:
        logger.error(f"Error occured calling PBS APIs: {e}")
        raise

    logger.info(f"Total initial leads found {len(inital_leads)}")

    # Filter leads
    filtered_leads = filter_leads(inital_leads, start_time, filtered_lead_types)
    logger.info(f"Total leads after filtering {len(filtered_leads)}")

    for lead in filtered_leads:
        contactId = lead.get("BuyerRef", None)
        vehicleId = None

        vehicles = lead.get("Vehicles", [])
        if len(vehicles) > 0:
            vehicleId = vehicles[0].get("VehicleRef", None)

        if contactId:
            try:
                lead["Contact_Info"] = api.call_contact_get(contactId, crm_dealer_id).get("Contacts")[0]
            except Exception as e:
                logger.warn(f"Error getting contact info, skipping lead {lead.get("DealId")}")
                continue

        if vehicleId:
            try:
                lead["Vehicle_Info"] = api.call_vehicle_get(vehicleId, crm_dealer_id).get("Vehicles")[0]
            except Exception as e:
                logger.warn(f"Error getting vehicle info, skipping lead {lead.get("DealId")}")
                continue

    logger.info(f"Total leads saved {len(filtered_leads)}")
    return filtered_leads

def filter_leads(leads: list, filtered_lead_types: list):
    """Filter leads by DealCreationDate."""
    filtered_leads = []
    for lead in leads:
        try:
           lead_type = lead.get("LeadType", "")

           # TODO: Uncomment next two lines when ready to implement source filtering
           # if lead_type in filtered_lead_types:
           #     filtered_leads.append(lead)

           # TODO: remove next line when ready to implement source filtering
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

        #TODO: pass in valid lead types to save as a list in the input event
        filtered_lead_types = body.get("filtered_lead_types", [])


        leads = fetch_new_leads(start_time, crm_dealer_id, filtered_lead_types)
        if not leads:
            logger.info(f"No new leads found for dealer with serial number {crm_dealer_id} for {start_time}")
            return

        save_raw_leads(leads, product_dealer_id)

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