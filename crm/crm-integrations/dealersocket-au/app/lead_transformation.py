"""
Transform the lead, and upload to CRM Shared Layer
"""

import logging
from datetime import datetime
from json import dumps, loads
from os import environ
from typing import Any
from uuid import uuid4

import boto3
import requests
from requests.auth import HTTPBasicAuth
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

ENVIRONMENT = environ.get("ENVIRONMENT")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sm_client = boto3.client("secretsmanager")


class DuplicateLeadError(Exception):
    """The exception is raised when a lead already exists in the CRM Shared Layer."""
    pass


def get_secret(secret_name, secret_key) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)
    return secret_data


def create_consumer(consumer_data, product_dealer_id, crm_api_key) -> dict:
    """Create consumer in db."""
    try:
        response = requests.post(
            url=f"https://{CRM_API_DOMAIN}/consumers",
            headers={"partner_id": UPLOAD_SECRET_KEY, "x_api_key": crm_api_key},
            params={"dealer_id": product_dealer_id},
            json=consumer_data,
        )
        response.raise_for_status()
        logger.info(f"CRM API /consumers responded with: {response.status_code}")

        unified_crm_consumer_id = response.json().get("consumer_id")
        if not unified_crm_consumer_id:
            error_msg = (
                "Error creating consumer: consumer_id not found in response"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        return unified_crm_consumer_id
    except Exception as e:
        logger.error(f"Error creating consumer from CRM API: {e}")
        raise


def create_lead(parsed_lead, crm_api_key) -> dict:
    """Create lead in db."""
    try:
        response = requests.post(
            url=f"https://{CRM_API_DOMAIN}/leads",
            headers={"partner_id": UPLOAD_SECRET_KEY, "x_api_key": crm_api_key},
            json=parsed_lead,
        )

        logger.info(f"CRM API /leads responded with: {response.status_code}")

        crm_lead_id = parsed_lead.get("crm_lead_id", None)

        if response.status_code == 409:
            logger.error(f"Could not create lead, Lead with crm_lead_id {crm_lead_id} already exists.")
            raise DuplicateLeadError(f"Lead with crm_lead_id {crm_lead_id} already exists.")

        response.raise_for_status()

        unified_crm_lead_id = response.json().get("lead_id")

        if not unified_crm_lead_id:
            logger.error(f"Error creating lead: {parse_lead}")
            raise Exception(f"Error creating lead: {parse_lead}")

        return unified_crm_lead_id
    except Exception as e:
        logger.error(f"Error creating lead from CRM API: {e}")
        raise


def parse_consumer(entity_data) -> dict:
    """Parse entity data to create crm consumer based on the CRM Integration Mapping"""
    try:
        customer_info = (
            entity_data.get('ShowCustomerInformation', {})
            .get('ShowCustomerInformationDataArea', {})
            .get('CustomerInformation', {})
            .get('CustomerInformationDetail', {})
            .get('CustomerParty', {})
            .get('SpecifiedPerson', {})
        )

        consumer = {
            "crm_consumer_id": (entity_data.get('ShowCustomerInformation', {})
                                .get('ShowCustomerInformationDataArea', {})
                                .get('CustomerInformation', {})
                                .get('CustomerInformationDetail', {})
                                .get('CustomerParty', {})
                                .get('PartyID', '')),
            "first_name": customer_info.get('GivenName', ''),
            "last_name": customer_info.get('FamilyName', ''),
            "middle_name": customer_info.get('MiddleName', ''),
            "email": customer_info.get('URICommunication', {}).get('URIID', ''),
            "phone": next((phone.get('CompleteNumber', '') for phone in customer_info.get('TelephoneCommunication', []) if phone.get('UseCode') == 'Mobile'), ''),
            "city": customer_info.get('PostalAddress', {}).get('CityName', ''),
            "country": "AU",  # Default value as per the CRM Integration Mapping
            "address": ' '.join(filter(None, [
                customer_info.get('PostalAddress', {}).get('LineOne', ''),
                customer_info.get('PostalAddress', {}).get('LineTwo', ''),
                customer_info.get('PostalAddress', {}).get('LineThree', '')
            ])),
            "postal_code": customer_info.get('PostalAddress', {}).get('Postcode', '')
        }

        return consumer
    except Exception as e:
        logger.error(f"Unexpected error parsing consumer data: {e}")
        raise


def parse_lead(event, carsales_data) -> dict:
    """
    Parse event data to create a lead based on the provided schema and mapping.
    """
    try:
        lead_status_mapping = {
            220: "Unqualified",
            221: "Up/Contacted",
            227: "Store Visit",
            222: "Demo Vehicle",
            223: "Write Up",
            224: "Pending F&I",
            225: "Sold",
            226: "Lost"
        }

        # If status does not match the mapping, then error is raised
        status = event.get('status')
        if status not in lead_status_mapping:
            raise ValueError(f"Lead status '{status}' is not recognized.")

        lead = {
            "crm_lead_id": event.get("eventId"),
            "lead_ts": event.get("insertDate"),
            "lead_status": lead_status_mapping.get(status),
            "lead_comment": carsales_data.get("Comments", ''),
            "lead_origin": "INTERNET",  # Hardcoded as per the mapping
            "lead_source": "CarSales",  # Hardcoded as per the mapping
            "vehicles_of_interest": [{
                "vin": event.get('vin', ''),
                "stock_number": event.get('stockNumber', ''),
                "mileage": event.get('currentMileage', 0),
                "make": event.get('make', ''),
                "model": event.get('model', ''),
                "year": event.get('year', ''),
            }],
            "salespersons": [
                {
                    "crm_salesperson_id": event.get('primaryAssigned', ''),
                    "first_name": carsales_data.get('Assignment', {}).get('Name', '').split()[0],
                    "last_name": carsales_data.get('Assignment', {}).get('Name', '').split()[1] if len(carsales_data.get('Name', '').split()) > 1 else '',
                    "email": carsales_data.get('Assignment', {}).get('Email', '')
                }
            ],
        }

        return lead
    except Exception as e:
        logger.error(f"Unexpected error parsing lead data: {e}")
        raise


def record_handler(record: SQSRecord):
    """
    Transform the lead, and upload to CRM Shared Layer
    """
    logger.info(f"Record: {record}")

    try:
        body = loads(record.body)

        entity_response = body.get("entity_response")
        event = body.get("event_response")
        product_dealer_id = body.get("product_dealer_id")
        carsales_data = body.get("carsales_data")

        # Get CRM API Key
        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        # Create consumer
        consumer_data = parse_consumer(entity_response)
        unified_consumer_id = create_consumer(
            consumer_data,
            product_dealer_id,
            crm_api_key
        )
        logger.info(f"Consumer created: {unified_consumer_id}")

        # Create leads
        lead_data = parse_lead(event, carsales_data)

        # Add consumer_id to lead_data
        lead_data["consumer_id"] = unified_consumer_id

        # Create the lead
        unified_lead_id = create_lead(lead_data, crm_api_key)
        logger.info(f"Lead created: {unified_lead_id}")
    except Exception as e:
        logger.error("Error processing record")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """
    Transform the lead, and upload to CRM Shared Layer
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
