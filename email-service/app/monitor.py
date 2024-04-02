import logging
from os import environ
import boto3
import json

LOGLEVEL = environ.get('LOGLEVEL', 'INFO')
FAILURE_QUEUE = environ.get('FAILURE_QUEUE')

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(LOGLEVEL))

SQS = boto3.client('sqs')

def lambda_handler(event, _):
    logger.info(event)
    ses_event = json.loads(event['Records'][0]['Sns']['Message'])
    if ses_event['eventType'] in ['Bounce', 'Reject']:
        SQS.send_message(
            QueueUrl=FAILURE_QUEUE,
            MessageBody=json.dumps(ses_event)
        )