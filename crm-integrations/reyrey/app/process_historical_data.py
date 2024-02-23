import xml.etree.ElementTree as ET
import json
import boto3
import gzip
from io import BytesIO
import logging
import os
from typing import Any

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

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
        logger.error(f'Error processing historical data: {str(e)}')
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
    """Upload the extracted prospect IDs to a different S3 bucket."""
    s3_key = f"configurations/test_REYREY.json"
    try:
        s3_response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        s3_content = s3_response['Body'].read().decode('utf-8')
        s3_object = json.loads(s3_content)

        if "historical_data" not in s3_object:
            s3_object["historical_data"] = {}

        s3_object["historical_data"][crm_dealer_id] = prospect_ids
        updated_s3_content = json.dumps(s3_object)
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=updated_s3_content)

    except Exception as e:
        logger.error(f"Failed to update S3 object. Partner: REYREY_CRM, Error: {str(e)}")
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
