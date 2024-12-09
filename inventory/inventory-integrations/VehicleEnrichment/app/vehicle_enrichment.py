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
    logger.info(f"Event records {event['Records']}")
    try:
        for record in event['Records']:
            sns_message = json.loads(record['body'])
            s3_event = sns_message['Records'][0]['s3']

            bucket_name = s3_event['bucket']['name']
            object_key = s3_event['object']['key']
        
            logger.info(f"Fetching object: {object_key} from bucket: {bucket_name}")
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            file_content = response['Body'].read().decode('unicode-escape')

            vehicle_data = parse_response(file_content)
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
        
        data = { 'statusCode': 200, 'body': 'Data processed successfully', 'results': merged_data}
    
    except Exception as e:
        logger.exception(f"Error processing event: {e}")
        data = {'statusCode': 500, 'body': 'Error processing data', 'results': []}

    logger.info(f"Returning retrieved data: {data}")
    return json.dumps(data)

def merge_data(vehicle_data, redbook_data):
    merged_data = {**vehicle_data, "options": [*redbook_data['results']]}
    return merged_data

def get_lambda_arn(logical_name, stack_name):
    try:
        response = cloudformation_client.describe_stack_resources(StackName=stack_name)
        
        for resource in response['StackResources']:
            if resource['ResourceType'] == 'AWS::Lambda::Function' and logical_name in resource['LogicalResourceId']:
                return resource['PhysicalResourceId']
        
        return None
    except Exception as e:
        print(f"Error fetching Lambda ARN from CloudFormation: {e}")
        return None
    
def parse_response(raw):
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw.strip('"')
    
    return json.loads(raw)

def get_redbook_payload(vehicle_data):
    specification_source = vehicle_data['Specification']['SpecificationSource']
    if specification_source != 'REDBOOK':
        raise Exception("Invalid SpecificationSource: Expected 'REDBOOK', but found {specification_source}")
    
    redbook_code = vehicle_data['Specification']['SpecificationCode']
    payload = {'redbookCode': redbook_code}

    logger.info(f"Successfully created payload with Redbook code: {payload}")
    return payload

def get_redbook_data(vehicle_data, environment):
    stack_name = f'redbook-{environment}'
    logical_name = 'RedBookDataFunction'
    lambda_arn = get_lambda_arn(logical_name, stack_name)
    payload = get_redbook_payload(vehicle_data)

    redbook_response = lambda_client.invoke(
        FunctionName = lambda_arn,
        InvocationType = 'RequestResponse',
        Payload = json.dumps(payload) 
    )

    raw_payload = redbook_response['Payload'].read().decode('unicode-escape')
        
    try:
        redbook_data = parse_response(raw_payload)
        logger.info(f"RedBook data: {redbook_data}")

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        raise

    return redbook_data