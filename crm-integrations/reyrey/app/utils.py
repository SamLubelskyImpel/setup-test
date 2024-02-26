import boto3
import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

SNS_TOPIC_ARN = environ.get('SNS_TOPIC_ARN')
AWS_REGION = environ.get('AWS_REGION')
ENVIRONMENT = environ.get("ENVIRONMENT")

sm_client = boto3.client("secretsmanager")


def send_email_notification(msg: str):
    """Send a notification message to the 'alert_client_engineering' SNS topic."""
    sns_client = boto3.client('sns', region_name=AWS_REGION)
    try:
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=msg, Subject='ReyRey CRM Failure Alert')
    except Exception as e:
        logger.exception(f'Failed to send notification {e}')
        raise
