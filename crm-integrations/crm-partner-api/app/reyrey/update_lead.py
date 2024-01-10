import json
import logging
import os
import boto3
from os import environ
from typing import Any, Dict
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
        logger.info(f"Lead status: {lead_status}")
    except Exception as e:
        logger.error(f"Failed to retrieve lead status from S3 config. Partner: {partner_name.upper()}, {e}")
        raise
    return lead_status


def get_secret(secret_name, secret_key) -> Any:
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
    
    logger.info(f"CRM API URL: {url}")

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



def lambda_handler(event, context):
    logger.info(event)
    try:
        # Extract the XML string from the event body
        xml_data = event['body']

        # Register the namespace
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
            raise RuntimeError("Unknown dealer id")

        crm_dealer_id = f"{store_number}_{area_number}_{dealer_number}"
        logger.info(f"CRM Dealer ID: {crm_dealer_id}")

        crm_api_key = get_secret(secret_name="crm-api", secret_key=UPLOAD_SECRET_KEY)["api_key"]

        identifier = record.find(".//ns:Identifier", namespaces=ns)

        crm_lead_id = identifier.find(".//ns:ProspectId", namespaces=ns).text

        logger.info(f"Prospect ID: {crm_lead_id}")
        
        lead_id = get_lead(crm_lead_id, crm_dealer_id, crm_api_key)

        logger.info(f"Lead ID: {lead_id}")

        event_id = record.find(".//ns:RCIDispositionEventId", namespaces=ns).text
        event_name = record.find(".//ns:RCIDispositionEventName", namespaces=ns).text

        lead_status = get_lead_status(event_id=str(event_id), partner_name="reyrey_crm")

        logger.info(f"Event ID: {event_id}")
        logger.info(f"Event Name: {event_name}")

        #update salesperson data if new salesperson is assigned
        if event_id == "30" or event_id == "31":
            new_salesperson = record.find(".//ns:RCIDispositionPrimarySalesperson", namespaces=ns).text
            logger.info(f"New Salesperson: {new_salesperson}")
            #update_salesperson_data(new_salesperson, crm_lead_id, )

        return {
            'statusCode': 200
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
