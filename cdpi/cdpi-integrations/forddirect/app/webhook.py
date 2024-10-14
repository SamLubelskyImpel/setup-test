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
TOPIC_ARN = os.environ.get('ALERT_CLIENT_ENGINEERING_TOPIC')

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

def send_to_queue(queue_url, file_type, file_path):
    """Call SQS queue to download file from specified SFTP path."""
    data = {
        "dealer_id": dealer_id,
        "end_dt_str": end_dt_str,
    }
    logger.info(f"Sending {data} to {queue_url}")
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=dumps(data)
    )

def authenticate_request(event, context):
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
        secret = SM_CLIENT.get_secret_value(
            SecretId=f"{'prod' if is_prod else 'test'}/cdpi-web"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.exception("Could not locate secret value")
        else:
            logger.exception("Unknown error occurred fetching secret")
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
        raise e
        
    try:
        secret = loads(secret["SecretString"])[str(partner_id)]
        secret_data = loads(secret)
    except KeyError:
        logger.exception(f"Access denied for partner_id {partner_id} to endpoint {endpoint}. Invalid partner id")
        message = {
            "statusCode": 401,
            "body": dumps(
                {
                    "message": "Unauthorized.",
                    "recv_timestamp": datetime.utcnow()
                    .replace(tzinfo=timezone.utc)
                    .isoformat(),
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }
        return authenticated, message

    authorized = api_key == secret_data["api_key"]

    if authorized:

        try:
            body = loads(event["body"])

            message = body.get("message", None)
            file_path = body.get("file_path", None)
            file_type = body.get("file_type", "missing")

            if file_type not in FILE_TYPES or not file_path or not message:
                message = {
                    "statusCode": 400,
                    "body": dumps(
                        {
                        "message": "Request body is missing or invalid.",
                        "recv_timestamp": datetime.utcnow()
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
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
                    "message": "Request body is missing or invalid.",
                    "recv_timestamp": datetime.utcnow()
                    .replace(tzinfo=timezone.utc)
                    .isoformat(),
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
    else:
        logger.exception(f"Access denied for partner_id {partner_id} to endpoint {endpoint}. Incorrect API key provided")
        message = {
            "statusCode": 401,
            "body": dumps(
                {
                    "message": "Unauthorized.",
                    "recv_timestamp": datetime.utcnow()
                    .replace(tzinfo=timezone.utc)
                    .isoformat(),
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }
        return authenticated, message

def lambda_handler(event, context):
    logger.info(event)
    message =  {
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
        authenticated, message = authenticate_request(event, context)
        if authenticated:
            headers = {k.lower(): v for k, v in event['headers'].items()}
            partner_id = headers.get('partner_id')
            send_to_queue(
                DOWNLOAD_QUEUE,
                partner_id,
                datetime.utcnow()
                .replace(tzinfo=timezone.utc)
                .isoformat(),
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