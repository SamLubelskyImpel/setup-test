import logging
from os import environ
import boto3
import json

LOGLEVEL = environ.get('LOGLEVEL', 'INFO')
SES_SOURCE = environ.get('SES_SOURCE')

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(LOGLEVEL))

S3 = boto3.client('s3')
SES = boto3.client('ses')

def lambda_handler(event, context):
    logger.info(event)
    
    for record in event['Records']:
        bucket, key = record['s3']['bucket']['name'], record['s3']['object']['key']
        obj = json.loads(S3.get_object(Bucket=bucket, Key=key).get('Body').read().decode('utf-8'))
        SES.send_email(
            Source=obj['from_address'],
            Destination={'ToAddresses': obj['recipients']},
            Message={'Subject': {'Data': obj['subject']}, 'Body': {'Text': {'Data': obj['body']}}},
            ReplyToAddresses=obj.get('reply_to', [])
        )
        