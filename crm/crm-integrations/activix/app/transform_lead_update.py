"""Update activix lead in the Impel CRM persistence layer."""
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
    BatchProcessor,
    EventType,
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
            params={"crm_dealer_id": crm_dealer_id, "integration_partner_name": "ACTIVIX"},
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


def update_lead(lead_id: str, data: dict, crm_api_key: str) -> Any:
    """Update lead through CRM API."""
    url = f'https://{CRM_API_DOMAIN}/leads/{lead_id}'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    response = requests.put(url, headers=headers, json=data)

    logger.info(f"CRM API Put Lead responded with: {response.status_code}")

    response.raise_for_status()
    response_data = response.json()
    return response_data


def process_salespersons(response_data, data, position_name):
    """Process salespersons from CRM API response."""
    new_first_name = data.get('first_name')
    new_last_name = data.get('last_name')

    logger.info(f"New Salesperson: {new_first_name} {new_last_name}")
    logger.info(f"Salesperson: {data}")

    for salesperson in response_data:
        if salesperson.get('crm_salesperson_id') == str(data.get('id')):
            salesperson['first_name'] = new_first_name
            salesperson['last_name'] = new_last_name
            salesperson['email'] = data.get('email')
            salesperson['is_primary'] = True
            return response_data

    return [create_or_update_salesperson(data, position_name)]


def create_or_update_salesperson(data, position_name):
    """Create or update salesperson."""
    logger.info(f"Creating or updating salesperson: {data}")
    logger.info(f"CRM Salesperson id: {data.get('id')}")
    return {
        "crm_salesperson_id": str(data.get('id')),
        "first_name": data.get('first_name'),
        "last_name": data.get('last_name'),
        "email": data.get('email'),
        # "phone": "",
        "position_name": position_name,
        "is_primary": True
    }


def update_lead_salespersons(salesperson, lead_id: str, crm_api_key: str, position_name) -> Any:
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
    logger.info(f"CRM API Get Salesperson response data: {response_data}")
    salespersons = process_salespersons(response_data, salesperson, position_name)
    logger.info(f"Processed salespersons: {salespersons}")
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

        crm_lead_id = str(json_data["id"])
        crm_dealer_id = str(json_data["account_id"])
        lead_status = json_data.get("status", "")

        # if status is not provided, then default value is Active. Activix considers 'Active' as having no status, whereas invalid, duplicate, lost are considered as having a status.
        if not lead_status:
            lead_status = "Active"

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]
        lead_id = get_lead(crm_lead_id, crm_dealer_id, crm_api_key)

        if not lead_id:
            logger.warning(f"Lead ID is not found in the CRM Shared layer: {crm_lead_id}. No update will be performed for this lead.")
            return

        data = {'metadata': {}}

        if lead_status:
            data['lead_status'] = lead_status
        else:
            logger.warning(f"Lead status is empty for CRM lead ID: {crm_lead_id}. No update will be performed for this field.")

        if json_data.get("bdc"):
            salesperson = update_lead_salespersons(json_data["bdc"], lead_id, crm_api_key, "BDC")
            data['salespersons'] = salesperson
        elif json_data.get("advisor"):
            salesperson = update_lead_salespersons(json_data["advisor"], lead_id, crm_api_key, "Advisor")
            data['salespersons'] = salesperson
        else:
            logger.warning(f"Salesperson info (BDC/Advisor) not included. CRM lead ID: {crm_lead_id}. No update will be performed for salespersons.")

        if 'lead_status' in data or 'salespersons' in data:
            update_lead(lead_id, data, crm_api_key)
            logger.info(f"Lead {crm_lead_id} updated successfully.")
        else:
            logger.info(f"No updates to apply for lead {crm_lead_id}.")

    except Exception as e:
        logger.error(f"Error transforming activix lead update record - {record}: {e}")
        logger.error("[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] ProductDealerId: {}\nLeadId: {}\nCrmDealerId: {}\nCrmLeadId: {}\nTraceback: {}".format(
            product_dealer_id, lead_id, crm_dealer_id, crm_lead_id, e)
        )
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw activix lead update data to the unified format."""
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
