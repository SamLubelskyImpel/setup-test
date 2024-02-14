"""Update momentum lead in the Impel CRM persistence layer."""
import json
from json import loads
import logging
import os
import boto3
from os import environ
from typing import Any
import requests
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    SqsFifoPartialProcessor,
    process_partial_response,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
SECRET_KEY = environ.get("SECRET_KEY")

sm_client = boto3.client('secretsmanager')
s3_client = boto3.client("s3")

def get_secret(secret_name: Any, secret_key: Any) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data

def get_lead(crm_lead_id, crm_dealer_id, crm_api_key):
    """Get existing lead from CRM API."""
    try:
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/leads/crm/{crm_lead_id}",
            headers={"partner_id": UPLOAD_SECRET_KEY, "x_api_key": crm_api_key},
            params={"crm_dealer_id": crm_dealer_id, "integration_partner_name": "MOMENTUM"},
        )
        status_code = response.status_code
        if status_code == 200:
            return response.json()["lead_id"]
        elif status_code == 404:
            return None
        else:
            raise Exception(f"Error getting existing lead from CRM API: {response.text}")

    except Exception as e:
        logger.error(f"Error getting existing lead from CRM API: {e}")
#         raise

def update_lead_status(lead_id: str, data: dict, crm_api_key: str) -> Any:
    """Update lead status through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/{lead_id}'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    response = requests.put(url, headers=headers, json=data)

    logger.info(f"CRM API Put Lead responded with: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Error updating lead with lead_id {lead_id}: {response.text}")
        raise

    response_data = response.json()
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
    return {
        "crm_salesperson_id": f"{last_name}, {first_name}",
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

    response = requests.get(url, headers=headers)
    logger.info(f"CRM API Get Salesperson responded with: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Error getting lead salespersons with lead_id {lead_id}: {response.text}")
        raise
        
    response_data = response.json()
    salespersons = process_salespersons(response_data, new_salesperson)
    return salespersons
    
def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record['body'])
        bucket = body["detail"]["bucket"]["name"]
        key = body["detail"]["object"]["key"]
        product_dealer_id = key.split('/')[2]

        logger.info(f"Product dealer id: {product_dealer_id}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        json_data = loads(content)
        logger.info(f"Raw data: {json_data}")

        crm_lead_id = json_data["id"]
        crm_dealer_id = json_data["dealerID"]
        contact_name = json_data["contactName"]
        lead_status = json_data["leadStatus"]
        lead_status_timestamp = json_data["leadStatusTimestamp"]

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        lead_id = get_lead(crm_lead_id, crm_dealer_id, crm_api_key)
        
        salesperson = update_lead_salespersons(contact_name, lead_id, crm_api_key)

        data = {
            'lead_status': lead_status,
            'metadata': {"leadStatusTimestamp": lead_status_timestamp},
            'salespersons': salesperson
        }

        update_lead_status(lead_id, data, crm_api_key)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Lead {crm_lead_id} updated successfully'})
        }

    except Exception as e:
        logger.error(f"Error transforming momentum lead update record - {record}: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw momentum lead update data to the unified format."""
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
