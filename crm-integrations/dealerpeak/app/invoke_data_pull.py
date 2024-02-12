"""Invoke DealerPeak data pull."""

import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
import requests
from requests.auth import HTTPBasicAuth
from uuid import uuid4
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    SqsFifoPartialProcessor,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")


def get_secrets():
    """Get DealerPeak API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]


def fetch_new_leads(start_time: str, end_time: str, crm_dealer_id: str):
    """Fetch new leads from DealerPeak CRM."""
    api_url, username, password = get_secrets()
    auth = HTTPBasicAuth(username, password)
    dealer_group_id, location_id = crm_dealer_id.split("__")

    # Get inital list of leads
    try:
        response = requests.get(
            url=f"{api_url}/dealergroup/{dealer_group_id}/location/{location_id}/leads",
            params={
                "deltaDate": start_time
            },
            auth=auth,
            timeout=3,
        )
        response.raise_for_status()
        inital_leads = response.json()

    except Exception as e:
        logger.error(f"Error occured calling DealerPeak APIs: {e}")
        raise

    logger.info(f"Total initial leads found {len(inital_leads)}")

    # Filter leads
    filtered_leads = filter_leads(inital_leads, start_time)
    logger.info(f"Total leads after filtering {len(filtered_leads)}")

    # Get new lead records
    new_leads = []
    for lead in filtered_leads:
        lead_id = lead.get("leadID")
        try:
            response = requests.get(
                url=f"{api_url}/dealergroup/{dealer_group_id}/lead/{lead_id}",
                auth=auth,
                timeout=3,
            )
            response.raise_for_status()
            lead_record = response.json()

            new_leads.append(lead_record)
        except Exception as e:
            logger.error(f"Error fetching lead {lead_id}. Skipping lead: {e}")
            continue

    logger.info(f"Total leads saved {len(new_leads)}")
    return new_leads


def filter_leads(leads: list, start_time: str):
    """Filter leads by dateCreated."""
    filtered_leads = []
    start_date = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
    for lead in leads:
        try:
            created_date = datetime.strptime(lead["dateCreated"], "%B, %d %Y %H:%M:%S")
            if created_date >= start_date:
                filtered_leads.append(lead)
        except Exception as e:
            logger.error(f"Error parsing dateCreated for lead {lead.get('leadID')}. Skipping lead: {e}")
            continue

    return filtered_leads


def save_raw_leads(leads: list, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = '%Y/%m/%d/%H/%M'
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/dealerpeak/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving leads to {s3_key}")
    s3_client.put_object(
        Body=dumps(leads),
        Bucket=BUCKET,
        Key=s3_key,
    )


def record_handler(record: SQSRecord):
    """Invoke DealerPeak data pull."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record['body'])

        start_time = body['start_time']
        end_time = body['end_time']
        crm_dealer_id = body['crm_dealer_id']
        product_dealer_id = body['product_dealer_id']

        leads = fetch_new_leads(start_time, end_time, crm_dealer_id)
        if not leads:
            logger.info(f"No new leads found for dealer {product_dealer_id} for {start_time} to {end_time}")
            return

        save_raw_leads(leads, product_dealer_id)

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        logger.error("[SUPPORT ALERT] Failed to Get Leads [CONTENT] ProductDealerId: {}\nDealerId: {}\nStartTime: {}\nEndTime: {}\nTraceback: {}".format(
            product_dealer_id, crm_dealer_id, start_time, end_time, e)
            )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Invoke DealerPeak data pull."""
    logger.info(f"Event: {event}")

    try:
        processor = SqsFifoPartialProcessor()
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
