"""Invoke Tekion CRM data pull."""

import boto3
import logging
from os import environ
import json
from json import dumps, loads
from typing import Any
import requests
from uuid import uuid4
from datetime import datetime
from access_token.s3 import get_token_from_s3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_INTEGRATION_SECRETS_ID = environ.get("CRM_INTEGRATION_SECRETS_ID")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")


def convert_to_epoch(timestamp_str):
    """Convert a timestamp in the format YYYY-MM-DDTHH:MM:SSZ to epoch time in milliseconds."""
    datetime_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    epoch_time_milliseconds = int(datetime_obj.timestamp() * 1000)
    return epoch_time_milliseconds


def get_credentials_from_secrets():
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=CRM_INTEGRATION_SECRETS_ID)
    content = json.loads(response["SecretString"])
    partner_content = content[SECRET_KEY]

    if isinstance(partner_content, str):
        partner_content = json.loads(partner_content)

    app_id = partner_content["app_id"]
    secret_key = partner_content["secret_key"]
    url = partner_content["url"]

    return app_id, secret_key, url


def fetch_new_leads(start_time: str, end_time: str, crm_dealer_id: str):
    """Fetch new leads from Tekion CRM."""
    token_from_s3 = get_token_from_s3()
    token = token_from_s3.token
    app_id, secret_key, url = get_credentials_from_secrets()

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "app_id": app_id,
        "dealer_id": crm_dealer_id,
        "Authorization": f"Bearer {token}",
    }
    params = {
        "createdStartTime": convert_to_epoch(start_time),
        "createdEndTime": convert_to_epoch(end_time)
    }

    api_url = f"{url}/openapi/v3.1.0/crm-leads"
    logger.info(f"Calling Tekion API: {api_url}", extra={"params": params})

    all_leads = []

    while True:
        logger.info(f"Getting leads with these parameters: {params}")
        try:
            response = requests.get(
                url=api_url,
                params=params,
                headers=headers,
            )
            response.raise_for_status()
            raw_data = response.json()
            metadata = raw_data["meta"]
            logger.info(f"Metadata from Tekion API response: {metadata}")
            leads = raw_data["data"]
            all_leads.extend(leads)

            # Check pagination info
            total_pages = raw_data["meta"]["pages"]
            current_page = raw_data["meta"]["currentPage"]
        
            if current_page >= total_pages:
                break

            # Update params for the next page
            next_page_key = raw_data["meta"].get("nextFetchKey", None)
            params["nextFetchKey"] = next_page_key

        except Exception as e:
            logger.error(f"Error occurred calling Tekion API: {e}")
            raise

    logger.info(f"Total leads found {len(all_leads)}")

    # Filter leads
    filtered_leads = filter_leads(all_leads, start_time, crm_dealer_id)
    logger.info(f"Total leads after filtering {len(filtered_leads)}")
    return filtered_leads


def filter_leads(leads: list, start_time: str, crm_dealer_id: str):
    """Filter leads by data source."""
    filtered_leads = []
    logger.info(leads)
    for lead in leads:
        try:
            lead_source = lead.get("source", {}).get("sourceType", "").upper()
            if lead_source == "INTERNET" or lead_source == "OEM":
                lead["impel_crm_dealer_id"] = crm_dealer_id
                filtered_leads.append(lead)
        except Exception as e:
            logger.error(f"Error parsing lead source for lead {lead.get('id')}. Skipping lead: {e}")
            continue

    return filtered_leads


def save_raw_leads(leads: list, product_dealer_id: str):
    """Save raw leads to S3."""
    format_string = '%Y/%m/%d/%H/%M'
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/tekion/{product_dealer_id}/{date_key}_{uuid4()}.json"
    logger.info(f"Saving leads to {s3_key}")
    s3_client.put_object(
        Body=dumps(leads),
        Bucket=BUCKET,
        Key=s3_key,
    )


def record_handler(record: SQSRecord):
    """Invoke Tekion CRM data pull."""
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
    """Invoke Tekion CRM data pull."""
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
