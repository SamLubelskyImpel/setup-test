import xml.etree.ElementTree as ET
import json
import boto3
import gzip
from io import BytesIO
import logging
import os
from typing import Any
from utils import send_email_notification

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = os.environ.get("ENVIRONMENT")
SECRET_KEY = os.environ.get("SECRET_KEY")

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """Extract prospect IDs from the historical data and upload them to S3."""
    try:
        logger.info(f'Event: {event}')
        bucket = event['bucket_name']
        key = event['file_name']
        xml_content = get_s3_object_content(bucket, key)

        if xml_content is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Could not retrieve or parse the .gz file.'})
            }

        root = ET.fromstring(xml_content)
        namespaces = {
            'ns': 'http://www.starstandards.org/STAR',
        }

        crm_dealer_id = get_crm_dealer_id(root, namespaces)
        prospect_ids = [prospect.find('ns:ProspectId', namespaces).text for prospect in root.findall('.//ns:Prospect', namespaces)]
        put_historical_data_to_s3(prospect_ids, bucket, crm_dealer_id)

        return {
            'statusCode': 200
        }
    except Exception as e:
        message = f'Error processing historical data: {str(e)}'
        logger.error(message)
        send_email_notification(message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error processing historical data.'})
        }


def get_s3_object_content(bucket, key):
    """Fetch and decompress the content of a .gz S3 object."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        compressed_file_io = BytesIO(response['Body'].read())

        with gzip.open(compressed_file_io, 'rt', encoding='utf-8') as gz:
            return gz.read()
    except Exception as e:
        logger.error(f"Error fetching or decompressing object from S3: {str(e)}")
        return None


def put_historical_data_to_s3(prospect_ids, bucket, crm_dealer_id):
    """Create or update the extracted prospect IDs as historical leads in a specified S3 bucket."""
    s3_key = f"historical_data/processed/reyrey_crm/{crm_dealer_id}.json"
    try:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=s3_key)
            existing_content = response['Body'].read().decode('utf-8')
            existing_data = json.loads(existing_content)
            historical_leads = set(existing_data.get("historical_leads", []))
        except s3_client.exceptions.NoSuchKey:
            logger.info(f"No existing data for {crm_dealer_id}, creating a new list.")
            historical_leads = set()

        historical_leads.update(prospect_ids)
        updated_data = {"historical_leads": list(historical_leads)}
        updated_content = json.dumps(updated_data)
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=updated_content)
        logger.info(f"Successfully updated historical leads for {crm_dealer_id}.")

    except Exception as e:
        logger.error(f"Failed to update S3 object. Partner: {SECRET_KEY}, Error: {str(e)}")
        raise


def get_crm_dealer_id(root: ET.Element, ns: Any) -> str:
    """Extract the CRM dealer ID from the XML content."""
    application_area = root.find(".//ns:ApplicationArea", namespaces=ns)

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
    return crm_dealer_id
