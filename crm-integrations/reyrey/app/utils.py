import boto3
import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

def send_email_notification(msg:str):
    """Send a notification message to the 'alert_client_engineering' SNS topic."""
    _ACCOUNT_ID = environ.get('ACCOUNT_ID', '143813444726')
    AWS_REGION = environ.get('AWS_REGION', 'us-east-1')
    SNS_CLIENT = boto3.client('sns', region_name=AWS_REGION)
    try:
        topic_arn = f'arn:aws:sns:{AWS_REGION}:{_ACCOUNT_ID}:alert_client_engineering'
        SNS_CLIENT.publish(TopicArn=topic_arn, Message=msg, Subject=msg)
    except Exception as e:
        logger.exception(f'Failed to send notification {e}')
        raise
