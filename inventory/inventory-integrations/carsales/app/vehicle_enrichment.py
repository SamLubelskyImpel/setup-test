import uuid
import json
import boto3
import logging
from datetime import datetime
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
cloudformation_client = boto3.client('cloudformation')

def lambda_handler(event, context):
    environment = environ.get('ENVIRONMENT', 'unknown')
    logger.info(f"Running in {environment} environment")

    failed_messages = []

    for record in event['Records']:
        try:
            sns_message = json.loads(record['body'])
            s3_event = sns_message['Records'][0]['s3']

            bucket_name = s3_event['bucket']['name']
            object_key = s3_event['object']['key']

            logger.info(f"Fetching object: {object_key} from bucket: {bucket_name}")
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response['Body'].read().decode('utf-8')

            logger.info(f"File Content: {file_content}")
            vehicle_data = json.loads(json.loads(file_content))
            logger.info(f"Vehicle data: {vehicle_data}")

            logger.info(f"Invoking RedBook Integration Lambda for object: {object_key}")
            redbook_data = get_redbook_data(vehicle_data, environment)

            merged_data = merge_data(vehicle_data, redbook_data)
            logger.info(f"Enriched data: {merged_data}")

            current_time = datetime.now()
            unique_id = str(uuid.uuid4())
            iso_timestamp = current_time.isoformat()

            year = current_time.strftime("%Y")
            month = current_time.strftime("%m")
            day = current_time.strftime("%d")

            impel_dealer_id = str(object_key).split("/")[2]

            merged_object_key = f"raw/carsales/{impel_dealer_id}/{year}/{month}/{day}/{iso_timestamp}_{unique_id}.json"
            logger.info(f"Saving enriched data to: {bucket_name}/{merged_object_key}")
            s3_client.put_object(Bucket=bucket_name, Key=merged_object_key, Body=json.dumps(merged_data))
        
        except Exception as e:
            logger.exception(f"Failed to process message {record['messageId']}: {e}")
            failed_messages.append({'itemIdentifier': record['messageId']})

    return {'batchItemFailures': failed_messages}

def merge_data(vehicle_data, redbook_data):
    merged_data = {**vehicle_data, "options": [*redbook_data['results']]}
    return merged_data

def parse_response(raw):
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw.strip('"')
    
    return json.loads(raw)

def get_redbook_payload(vehicle_data):
    specification_source = vehicle_data['Specification']['SpecificationSource']
    if specification_source != 'REDBOOK':
        raise Exception(f"Invalid SpecificationSource: Expected 'REDBOOK', but found {specification_source}")
    
    redbook_code = vehicle_data['Specification']['SpecificationCode']
    payload = {'redbookCode': redbook_code}

    logger.info(f"Successfully created payload with Redbook code: {payload}")
    return payload

def get_redbook_data(vehicle_data, environment):
    function_name = f'redbook-{environment}-RedBookDataFunction'
    payload = get_redbook_payload(vehicle_data)

    try:
        redbook_response = lambda_client.invoke(
            FunctionName = function_name,
            InvocationType = 'RequestResponse',
            Payload = json.dumps(payload) 
        )

        raw_payload = redbook_response['Payload'].read().decode('utf-8')
        logger.info(f"Redbook Lambda Response: {raw_payload}")


    except Exception as e:
        logger.exception(f"Failed to get Redbook Data {redbook_response}: {e}")
        raise

    try:
        redbook_data = json.loads(json.loads(raw_payload))
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode Redbook data: {type(raw_payload)} {e}")
        raise Exception(f"Invalid JSON response from Redbook Lambda: {e}")

    if 'results' not in redbook_data or not redbook_data['results']:
        logger.warning("Redbook data contains no results.")
        raise Exception("No results found in Redbook data.")
        

    return redbook_data
