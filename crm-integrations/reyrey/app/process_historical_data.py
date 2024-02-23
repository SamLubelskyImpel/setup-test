import xml.etree.ElementTree as ET
import json
import boto3
import gzip
from io import BytesIO
import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

s3 = boto3.client('s3')


def lambda_handler(event, context):
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
        
        # Find all ProspectId elements and extract their text
        prospect_ids = [prospect.find('ns:ProspectId', namespaces).text for prospect in root.findall('.//ns:Prospect', namespaces)]
        logger.info(f'Prospect IDs: {prospect_ids}')
        # Save the prospect IDs to a config file

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
        response = s3.get_object(Bucket=bucket, Key=key)
        compressed_file_io = BytesIO(response['Body'].read())

        with gzip.open(compressed_file_io, 'rt', encoding='utf-8') as gz:
            return gz.read()
    except Exception as e:
        print(f"Error fetching or decompressing object from S3: {str(e)}")
        return None
