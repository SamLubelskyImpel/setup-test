"""Invoke Kawasaki data download for each dealer."""
import logging
from json import dumps, loads
from os import environ

import boto3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
SQS_CLIENT = boto3.client("sqs")
S3_CLIENT = boto3.client("s3")
SNS_CLIENT = boto3.client("sns")
QUEUE_URL = environ["KAWASAKI_DL_QUEUE_URL"]
ALERT_TOPIC_ARN = environ["ALERT_TOPIC_ARN"]
KAWASAKI_DATA_BUCKET = environ["KAWASAKI_DATA_BUCKET"]


def send_to_queue(web_provider, dealer_config):
    """Call SQS queue to invoke data pull for specific dealer."""
    data = {
        "web_provider": web_provider,
        "dealer_config": dealer_config,
    }
    logger.info(f"Sending {data} to {QUEUE_URL}")
    SQS_CLIENT.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=dumps(data)
    )


def load_json_s3(bucket_name, key):
    """Load json from an S3 file given a bucket name and key."""
    response = S3_CLIENT.get_object(Bucket=bucket_name, Key=key)
    json_content = response['Body'].read().decode('utf-8')
    return loads(json_content)


def lambda_handler(event, context):
    """Invoke Scheduled Kawasaki data pulls per dealer."""
    try:
        kawasaki_config = load_json_s3(KAWASAKI_DATA_BUCKET, "kawasaki_config.json")
        for web_provider, dealer_configs in kawasaki_config.items():
            for dealer_config in dealer_configs:
                send_to_queue(web_provider, dealer_config)
    except Exception:
        message = f"Error invoking Kawasaki downloads {event}"
        logger.exception(message)
        SNS_CLIENT.publish(
            TopicArn=ALERT_TOPIC_ARN,
            Message=message
        )
        raise
