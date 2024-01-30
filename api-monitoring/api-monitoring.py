import json
import logging
from os import environ
import boto3
from requests.exceptions import RequestException
import requests
import time
import base64

# Initialize logger
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

# Initialize Boto3 S3 client
s3_client = boto3.client('s3')

# Retrieve the environment variable
env = environ.get("ENVIRONMENT")

def get_api_config():
    """Fetch the API configuration from an S3 bucket based on the environment."""
    config_bucket = "api-monitoring-us-east-1-prod" if env == 'prod' else 'api-monitoring-us-east-1-test'
    config_key = 'api_config.json'      # The S3 key for the config file
    
    try:
        response = s3_client.get_object(Bucket=config_bucket, Key=config_key)
        api_config = json.loads(response['Body'].read().decode('utf-8'))
        return api_config
    except Exception as e:
        logger.error(f"Error fetching API configuration: {e}")
        raise e


def call_api(api):
    """Make an API call and return the response."""
    headers = api.get('headers', {})
    logger.info(api)

    try:
        response = requests.request(
            method=api['method'],
            url=api['url'],
            headers=headers,
            json=api.get('body', {}),
            params=api.get('params', {})
        )
        return response
    except RequestException as e:
        return None

def lambda_handler(event, context):
    api_config = get_api_config()
    results = []
    logger.info('hi dev dayan2')
    
    for api in api_config:
        response = call_api(api)
        
        expected_status = 401 if api.get('use_unauthorized_auth') else 200
        if response.status_code == expected_status:
            status = 'success' if response.status_code == 200 else 'unauthorized success'
            results.append({'name': api['name'], 'status': status})
        else:
            status_code = response.status_code if response else 'N/A'
            reason = response.reason if response else 'No response'
            results.append({
                'name': api['name'],
                'status': 'failure',
                'code': status_code,
                'reason': reason
            })
            # Here you would integrate with your alerting system if necessary.

    logger.info(json.dumps(results, indent=2))
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }
