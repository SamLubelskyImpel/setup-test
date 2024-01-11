"""Update lead in the Impel CRM persistence layer."""
import json
import logging
import os
import boto3
import uuid
from os import environ
from typing import Any
import xml.etree.ElementTree as ET
import requests
from requests.exceptions import HTTPError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
PARTNER_NAME = environ.get("PARTNER_NAME")

sm_client = boto3.client('secretsmanager')
s3_client = boto3.client("s3")


def get_lead_status(event_id: str, partner_name: str) -> Any:
    """Get lead status from S3."""
    s3_key = f"configurations/{ENVIRONMENT}_{partner_name.upper()}.json"
    try:
        s3_object = json.loads(
                s3_client.get_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )
        lead_updates = s3_object.get("lead_updates")
        lead_status = lead_updates.get(event_id)
    except Exception as e:
        logger.error(f"Failed to retrieve lead status from S3 config. Partner: {partner_name.upper()}, {e}")
        raise
    return lead_status


def get_secret(secret_name: Any, secret_key: Any) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data


def get_lead(crm_lead_id: str, crm_dealer_id: str, crm_api_key: str) -> Any:
    """Get lead by crm lead id through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/crm/{crm_lead_id}?crm_dealer_id={crm_dealer_id}'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except HTTPError as http_err:
        if response.status_code == 404:
            logger.error(f"Lead not found: {http_err}")
            raise ValueError(f"Lead not found: {crm_lead_id}") from None
        else:
            # Handle other HTTP errors
            logger.error(f"HTTP error occurred: {http_err}")
            raise
    except Exception as err:
        # Handle other exceptions, such as a connection error
        logger.error(f"Error occurred: {err}")
        raise

    response_data = response.json()
    logger.info(f"CRM API Response: {response_data}")
    lead_id = response_data.get('lead_id')
    return lead_id


def update_lead_status(lead_id: str, data: dict, crm_api_key: str) -> Any:
    """Update lead status through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/{lead_id}'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    try:
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
    except HTTPError as http_err:
        if response.status_code == 404:
            logger.error(f"Lead not found: {http_err}")
            raise ValueError(f"Lead not found: {lead_id}") from None
        else:
            # Handle other HTTP errors
            logger.error(f"HTTP error occurred: {http_err}")
            raise
    except Exception as err:
        # Handle other exceptions, such as a connection error
        logger.error(f"Error occurred: {err}")
        raise

    response_data = response.json()
    logger.info(f"CRM API Response: {response_data}")
    return response_data


def process_salespersons(response_data, new_salesperson):
    """Process salespersons from CRM API response."""
    new_first_name, new_last_name = new_salesperson.split()
    logger.info(f"New Salesperson: {new_first_name} {new_last_name}")

    if not response_data:
        logger.info("No salespersons found for this lead.")
        return [create_or_update_salesperson(new_salesperson)]

    return [
        create_or_update_salesperson(new_salesperson)
        if salesperson.get('is_primary') and (salesperson.get('first_name') != new_first_name or salesperson.get('last_name') != new_last_name)
        else salesperson for salesperson in response_data
    ]


def create_or_update_salesperson(new_salesperson):
    first_name, last_name = new_salesperson.split()
    guid = str(uuid.uuid4())
    return {
        "crm_salesperson_id": f"Impel_generated_{guid}",
        "first_name": first_name,
        "last_name": last_name,
        "email": "",
        "phone": "",
        "position_name": "Primary Salesperson",
        "is_primary": True
    }


def update_lead_salespersons(new_salesperson: str, lead_id: str, crm_api_key: str) -> Any:
    """Update lead salespersons through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/{lead_id}/salespersons'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except HTTPError as http_err:
        if response.status_code == 404:
            logger.error(f"Lead not found: {http_err}")
            raise ValueError(f"Lead not found: {lead_id}") from None
        else:
            # Handle other HTTP errors
            logger.error(f"HTTP error occurred: {http_err}")
            raise
    except Exception as err:
        # Handle other exceptions, such as a connection error
        logger.error(f"Error occurred: {err}")
        raise

    response_data = response.json()
    logger.info(f"CRM API Get Salesperson Response: {response_data}")

    salespersons = process_salespersons(response_data, new_salesperson)

    return salespersons


def lambda_handler(event, context):
    logger.info(event)
    try:
        xml_data = event['body']

        ns = {'ns': 'http://www.starstandards.org/STAR'}
        ET.register_namespace('', ns['ns'])

        root = ET.fromstring(xml_data)

        if root.tag != '{http://www.starstandards.org/STAR}rey_ImpelCRMPublishLeadDisposition':
            raise ValueError("Invalid XML format")

        application_area = root.find(".//ns:ApplicationArea", namespaces=ns)
        record = root.find(".//ns:Record", namespaces=ns)

        dealer_number = None
        store_number = None
        area_number = None
        if application_area is not None:
            sender = application_area.find(".//ns:Sender", namespaces=ns)
            if sender is not None:
                dealer_number = sender.find(".//ns:DealerNumber", namespaces=ns).text
                store_number = sender.find(".//ns:StoreNumber", namespaces=ns).text
                area_number = sender.find(".//ns:AreaNumber", namespaces=ns).text

        if not dealer_number and not store_number and not area_number:
            raise ValueError("Unknown dealer id. DealerNumber, StoreNumber, and AreaNumber are missing.")

        crm_dealer_id = f"{store_number}_{area_number}_{dealer_number}"

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        identifier = record.find(".//ns:Identifier", namespaces=ns)
        crm_lead_id = identifier.find(".//ns:ProspectId", namespaces=ns).text
        
        event_id = record.find(".//ns:RCIDispositionEventId", namespaces=ns).text

        lead_id = get_lead(crm_lead_id, crm_dealer_id, crm_api_key)

        lead_status = get_lead_status(event_id=str(event_id), partner_name=PARTNER_NAME)

        data = {
            'lead_status': lead_status
        }

        # update salesperson data if new salesperson is assigned
        if event_id == "30" or event_id == "31":
            new_salesperson = record.find(".//ns:RCIDispositionPrimarySalesperson", namespaces=ns).text
            salespersons = update_lead_salespersons(new_salesperson, lead_id, crm_api_key)
            data['salespersons'] = salespersons

        update_lead_status(lead_id, data, crm_api_key)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Lead {crm_lead_id} updated successfully'})
        }

    except ET.ParseError:
        # Handle XML parsing errors
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid XML'})
        }
    except ValueError as e:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": str(e)})
        }
