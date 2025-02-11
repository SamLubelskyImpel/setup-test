import logging
from json import loads, dumps
from os import environ
import boto3

DOWNLOAD_QUEUE = environ.get("DOWNLOAD_QUEUE")
TOPIC_ARN = environ.get('ALERT_CLIENT_ENGINEERING_TOPIC')
FILE_TYPES = [
    "consumer_profile_summary",
    "consumer_profile_summary_enterprise",
    "consumer_signal_detail",
    "dealer_attributes",
    "inventory",
    "pii_attributes",
    "pii_match",
    "dsr",
    "other"
]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")


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


def validate_request_body(event):
    """Validate the request body of the incoming webhook notification based on the OAS specification."""
    logger.info(event)

    authenticated = False
    try:
        body = loads(event["body"])

        file_path = str(body["file_path"])
        file_type = str(body["file_type"])

        if not file_path or file_type not in FILE_TYPES:
            message = {
                "statusCode": 400,
                "body": dumps(
                    {
                        "message": "File path not provided or file type is invalid."
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
                "message": "Accepted."
            }
        ),
        "headers": {"Content-Type": "application/json"},
    }
    authenticated = True
    return authenticated, message


def lambda_handler(event, context):
    logger.info(event)

    try:
        verified, message = validate_request_body(event)
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
        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "message": "Internal Server Error. Please contact Impel support."
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }


def notify_client_engineering(error_message):
    """Send a notification to the client engineering SNS topic."""
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="Ford Direct SFTP Notify Webhook Error",
        Message=str(error_message),
    )
    return
