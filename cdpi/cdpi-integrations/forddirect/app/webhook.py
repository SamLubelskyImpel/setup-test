import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ
from botocore.exceptions import ClientError
import boto3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")
DOWNLOAD_QUEUE = environ.get("DOWNLOAD_QUEUE")
TOPIC_ARN = environ.get('ALERT_CLIENT_ENGINEERING_TOPIC')

is_prod = environ.get("ENVIRONMENT", "test") == "prod"

FILE_TYPES = [ 
    "consumer_profile_summary", 
    "consumer_signal_detail", 
    "dealer_attributes", 
    "inventory", 
    "pii_attributes", 
    "pii_match", 
    "other" 
    ]

def send_to_queue(queue_url, partner_id, file_type, file_path):
    """Call SQS queue to download file from specified SFTP path."""
    data = {
        "partner_id": partner_id,
        "file_path": file_path,
        "file_type": file_type
    }
    logger.info(f"Sending {data} to {queue_url}")
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=dumps(data)
    )

def verify_request_body(event):
    """Take in the API_KEY/partner_id pair sent to the API Gateway and verifies against secrets manager. 
    Then check that the request body contains no errors"""
    logger.info(event)

    endpoint = event["path"]

    headers = {k.lower(): v for k, v in event['headers'].items()}
    partner_id = headers.get('partner_id')
    api_key = headers.get('x_api_key')

    SM_CLIENT = boto3.client("secretsmanager")

    authenticated = False
    message = {
        "statusCode": 500,
        "body": dumps(
            {
                "message": "Internal Server Error. Please contact Impel support.",
                "recv_timestamp": datetime.utcnow()
                .replace(tzinfo=timezone.utc)
                .isoformat(),
            }
        ),
        "headers": {"Content-Type": "application/json"},
    }
    
    try:
        body = loads(event["body"])

        file_path = body.get("file_path", None)
        file_type = body.get("file_type", "missing")

        if file_type not in FILE_TYPES or not file_path:
            message = {
                "statusCode": 400,
                "body": dumps(
                    {
                    "message": "Request body is missing or invalid."
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }
            return authenticated, message
    except Exception as e:
        logger.exception(e)
        message = {
            "statusCode": 400,
            "body": dumps(
                {
                "message": "Request body is missing or invalid."
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }
        return authenticated, message

    message = {
        "statusCode": 200,
        "body": dumps(
            {
                "message": "Accepted.",
                "recv_timestamp": datetime.utcnow()
                .replace(tzinfo=timezone.utc)
                .isoformat(),
            }
        ),
        "headers": {"Content-Type": "application/json"},
    }
    authenticated = True
    return authenticated, message

def lambda_handler(event, context):
    logger.info(event)

    try:
        verified, message = verify_request_body(event)
        if verified:
            headers = {k.lower(): v for k, v in event['headers'].items()}
            partner_id = headers.get('partner_id')
            body = loads(event["body"])

            file_path = body.get("file_path", None)
            file_type = body.get("file_type", "missing")

            send_to_queue(
                DOWNLOAD_QUEUE,
                partner_id,
                file_type,
                file_path
            )
        return message
        
    except Exception as e:
        logger.exception(f"Error invoking ford direct {event}")
        notify_client_engineering(e)
        raise 

def notify_client_engineering(error_message):
    """Send a notification to the client engineering SNS topic."""
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="Ford Direct SFTP Notify Webhook Error",
        Message=str(error_message),
    )
    return