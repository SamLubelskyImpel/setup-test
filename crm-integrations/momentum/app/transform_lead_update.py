"""Update momentum lead in the Impel CRM persistence layer."""
import json
from json import loads
import logging
import os
import boto3
import uuid
from os import environ
from typing import Any
from datetime import datetime
import xml.etree.ElementTree as ET
import requests
from requests.exceptions import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def get_lead(crm_lead_id: str, crm_dealer_id: str, crm_consumer_id: str, crm_api_key: str) -> Any:
    """Get lead by crm lead id through CRM API."""
    queryStringParameters = f"crm_dealer_id={crm_dealer_id}&integration_partner_name={SECRET_KEY}"
    if crm_consumer_id:
        queryStringParameters += f"&crm_consumer_id={crm_consumer_id}"

    url = f'https://{CRM_API_DOMAIN}/leads/crm/{crm_lead_id}?{queryStringParameters}'

    headers = {
        'partner_id': UPLOAD_SECRET_KEY,
        'x_api_key': crm_api_key
    }

    response = requests.get(url, headers=headers)

    logger.info(f"CRM API Get Lead responded with: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Error getting lead with crm_lead_id {crm_lead_id}: {response.text}")
        raise

    response_data = response.json()
    lead_id = response_data.get('lead_id')
    return lead_id


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

        # Parse the XML content
        root = ET.fromstring(content)

        ns = {'ns': 'http://www.starstandards.org/STAR'}
        ET.register_namespace('', ns['ns'])

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

        crm_dealer_id = f"{store_number}_{area_number}_{dealer_number}"
        logger.info(f"CRM Dealer ID: {crm_dealer_id}")

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        identifier = record.find(".//ns:Identifier", namespaces=ns)
        crm_lead_id = identifier.find(".//ns:ProspectId", namespaces=ns).text
        logger.info(f"CRM Lead ID: {crm_lead_id}")

        crm_consumer_id = identifier.find(".//ns:NameRecId", namespaces=ns)
        if crm_consumer_id is not None:
            crm_consumer_id = crm_consumer_id.text

        event_id = record.find(".//ns:RCIDispositionEventId", namespaces=ns).text
        event_name = record.find(".//ns:RCIDispositionEventName", namespaces=ns).text
        logger.info(f"Event ID: {event_id}")
        logger.info(f"Event Name: {event_name}")

        lead_id = get_lead(crm_lead_id, crm_dealer_id, crm_consumer_id, crm_api_key)
        logger.info(f"Lead ID: {lead_id}")

        lead_status = get_lead_status(event_id=str(event_id), partner_name=SECRET_KEY)

        data = {
            'lead_status': lead_status,
            'metadata': {"updatedCrmLeadStatus": event_name}
        }

        salespersons = {}
        # update salesperson data if new salesperson is assigned
        # TODO: add logic to event id 29 once it's finalized
        if event_id == "30":
            new_salesperson = record.find(".//ns:RCIDispositionPrimarySalesperson", namespaces=ns)
            if new_salesperson is not None:
                new_salesperson = record.find(".//ns:RCIDispositionPrimarySalesperson", namespaces=ns).text
            else:
                raise Exception("Field with salesperson name is not provided.")

            salespersons = update_lead_salespersons(new_salesperson, lead_id, crm_api_key)
            data['salespersons'] = salespersons

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
        logger.info('hi dev dayan')
        # processor = SqsFifoPartialProcessor()
        # result = process_partial_response(
        #     event=event,
        #     record_handler=record_handler,
        #     processor=processor,
        #     context=context
        # )
        # return result

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
