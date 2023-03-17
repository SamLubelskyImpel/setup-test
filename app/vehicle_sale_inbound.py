"""Runs the template fill here API"""
import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())

"""
Event: 
{
    'resource': '/dms-data-service/v1',
    'path': '/dms-data-service/v1',
    'httpMethod': 'POST',
    'headers': {
        'accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate',
        'client_id': 'test1',
        'Content-Type': 'application/json',
        'Host': 'dms.testenv.impel.io',
        'User-Agent': 'python-requests/2.28.2',
        'X-Amzn-Trace-Id': 'Root=1-64139dc2-34c65e1d000742090c53f187',
        'X-Forwarded-For': '74.101.154.242',
        'X-Forwarded-Port': '443',
        'X-Forwarded-Proto': 'https',
        'x_api_key': 'test1',
        },
    'multiValueHeaders': {
        'accept': ['application/json'],
        'Accept-Encoding': ['gzip, deflate'],
        'client_id': ['test1'],
        'Content-Type': ['application/json'],
        'Host': ['dms.testenv.impel.io'],
        'User-Agent': ['python-requests/2.28.2'],
        'X-Amzn-Trace-Id': ['Root=1-64139dc2-34c65e1d000742090c53f187'
                            ],
        'X-Forwarded-For': ['74.101.154.242'],
        'X-Forwarded-Port': ['443'],
        'X-Forwarded-Proto': ['https'],
        'x_api_key': ['test1'],
        },
    'queryStringParameters': None,
    'multiValueQueryStringParameters': None,
    'pathParameters': None,
    'stageVariables': None,
    'requestContext': {
        'resourceId': 'dg9s3y',
        'authorizer': {'principalId': 'test1',
                       'integrationLatency': 1600},
        'resourcePath': '/dms-data-service/v1',
        'operationName': 'dms-data-service',
        'httpMethod': 'POST',
        'extendedRequestId': 'B5WWfGcFoAMF-_A=',
        'requestTime': '16/Mar/2023:22:52:50 +0000',
        'path': '/dms-data-service/v1',
        'accountId': '901863237878',
        'protocol': 'HTTP/1.1',
        'stage': 'stage',
        'domainPrefix': 'dms',
        'requestTimeEpoch': 1679007170857,
        'requestId': '80afe485-2835-4e0b-8dfe-7c040ab1941e',
        'identity': {
            'cognitoIdentityPoolId': None,
            'accountId': None,
            'cognitoIdentityId': None,
            'caller': None,
            'sourceIp': '74.101.154.242',
            'principalOrgId': None,
            'accessKey': None,
            'cognitoAuthenticationType': None,
            'cognitoAuthenticationProvider': None,
            'userArn': None,
            'userAgent': 'python-requests/2.28.2',
            'user': None,
            },
        'domainName': 'dms.testenv.impel.io',
        'apiId': 'lmz3es8mla',
        },
    'body': '{"sample_string": "12345ABC", "sample_obj": {"sample_string": "12345ABC"}}',
    'isBase64Encoded': False,
    }
"""

def lambda_handler(event, context):
    """Run template fill here API."""
    logger.info(f'Event: {event}')
    try:
        message_body = loads(event['body'])
        headers = event['headers']

        logger.info(f'Received body {message_body} and headers {headers}')

        return {
            'statusCode': '200',
            'body': dumps({
                'received_date_utc': datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat()
            })
        }
    except Exception:
        logger.exception('Error running template fill here api.')
        raise
