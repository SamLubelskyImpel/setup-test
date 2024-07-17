"""
Redrive messages from a DLQ
Usage: python redrive_dlq.py -e {Environment} -d {DLQ Name}
"""

import boto3
import logging
from argparse import ArgumentParser
import time

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROFILES = {
    'test': 'unified-test',
    'prod': 'devadmin'
}

ACCOUNTS = {
    'test': '143813444726',
    'prod': '196800776222'
}

parser = ArgumentParser(description='Redrive messages from DLQ')
parser.add_argument('-e', '--environment', choices=['test', 'prod'], required=True, help='Environment to redrive messages')
parser.add_argument('-d', '--dlq', required=True, help='DLQ Name')

args = parser.parse_args()
environment = args.environment
dlq = args.dlq

boto3.setup_default_session(profile_name=PROFILES[environment])
sqs = boto3.client('sqs')
dlq_arn = f'arn:aws:sqs:us-east-1:{ACCOUNTS[environment]}:{dlq}'
logger.info(f'Redriving messages from {dlq_arn}')

try:
    queue = sqs.get_queue_attributes(
        QueueUrl=f'https://sqs.us-east-1.amazonaws.com/{ACCOUNTS[environment]}/{dlq}',
        AttributeNames=['ApproximateNumberOfMessages']
    )

    logger.info(f'DLQ has {queue["Attributes"]["ApproximateNumberOfMessages"]} messages')
    sqs.start_message_move_task(SourceArn=dlq_arn)
    logger.info('Redrive initiated successfully')

    while True:
        progress = sqs.list_message_move_tasks(
            SourceArn=dlq_arn
        )

        logger.info(f'Redrive progress: {progress["Results"][0]}')
        if progress['Results'][0]['Status'] != 'RUNNING':
            break

        time.sleep(3)
except:
    logger.exception('Error redriving messages')

