import logging
from os import environ
import boto3
import json
from uuid import uuid4

LOGLEVEL = environ.get('LOGLEVEL', 'INFO')
SES_SOURCE = environ.get('SES_SOURCE')
CONFIG_SET = environ.get('CONFIG_SET')

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(LOGLEVEL))

S3 = boto3.client('s3')
SES = boto3.client('ses')

def lambda_handler(event, _):
    logger.info(event)
    
    record = event['Records'][0]
    bucket, key = record['s3']['bucket']['name'], record['s3']['object']['key']
    obj = json.loads(S3.get_object(Bucket=bucket, Key=key).get('Body').read().decode('utf-8'))
    s3_path = f's3://{bucket}/{key}'
    uid = str(uuid4())
    
    logger.info(f'S3 file: {s3_path}, uid: {uid}')  # just to help with debugging while the monitoring tool is not implemented
    
    SES.send_email(
        Source=obj['from_address'],
        Destination={'ToAddresses': obj['recipients']},
        Message={'Subject': {'Data': obj['subject']}, 'Body': {'Text': {'Data': obj['body']}}},
        ReplyToAddresses=obj.get('reply_to', []),
        ConfigurationSetName=CONFIG_SET,
        Tags=[{
            'Name': 'uid',
            'Value': uid
        }]
    )
   