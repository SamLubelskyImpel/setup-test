"""Process lead updates."""

import boto3
import logging
from os import environ
from json import loads
from typing import Any
import requests
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")
CRM_API_URL = environ.get("CRM_API_URL")
PARTNER_ID_MAPPINGS = loads(environ.get("PARTNER_ID_MAPPINGS", "{}"))

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")


def get_secrets(partner_id: str):
    """Get CRM API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret["SecretString"])[str(partner_id)]
    secret_data = loads(secret)

    return secret_data["api_key"]


def send_update_request(lead_id: str, request_body: dict, partner_id: str) -> Any:
    """Send update request to CRM API."""
    if not request_body:
        logger.info(f"No updates for lead {lead_id}")
        return

    api_key = get_secrets(partner_id)
    response = requests.put(
        url=f"{CRM_API_URL}leads/{lead_id}",
        headers={
            "Content-Type": "application/json",
            "partner_id": partner_id,
            "x_api_key": api_key
        },
        json=request_body
    )
    logger.info(f"CRM API responded with: {response.status_code}")
    if response.status_code != 200:
        logger.error(f"Error updating lead {lead_id}: {response.text}")
        raise

def get_partner_id(sender_id: str) -> str:
    """Get PARTNER_ID based on sender"""
    if "dealerpeak" in sender_id.lower():
        return PARTNER_ID_MAPPINGS.get("dealerpeak", "impel")
    elif "tekion" in sender_id.lower():
        return PARTNER_ID_MAPPINGS.get("tekion", "impel")
    elif "pbs" in sender_id.lower():
        return PARTNER_ID_MAPPINGS.get("pbs", "impel")
    return "impel"

def record_handler(record: SQSRecord):
    """Process lead updates."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])

        lead_id = body["lead_id"]
        status = body.get("status", "")
        salespersons = body.get("salespersons", [])
        sender_id = record["attributes"]["SenderId"]
        logger.info(f"Sender ID: {sender_id}")

        # Make request to CRM API to update lead
        request_body = {}
        if status:
            request_body.update({"lead_status": status})

        lead_salespersons = []
        for person in salespersons:
            lead_salespersons.append({
                "crm_salesperson_id": person["crm_salesperson_id"],
                "first_name": person.get("first_name", ""),
                "last_name": person.get("last_name", ""),
                "email": person.get("email", ""),
                "phone": person.get("phone", ""),
                "position_name": person.get("position_name", ""),
                "is_primary": person.get("is_primary") if isinstance(person.get("is_primary"), bool) else False
            })
        if lead_salespersons:
            request_body.update({"salespersons": lead_salespersons})

        partner_id = get_partner_id(sender_id=sender_id)
        send_update_request(lead_id=lead_id, request_body=request_body, partner_id=partner_id)

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Process lead updates."""
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
