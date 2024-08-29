"""Services on the microservice logic for Repair Orders from Quiter."""

import logging
import boto3
import random

from json import dumps
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

SQS_CLIENT = boto3.client("sqs")
INVOKE_QUEUE = environ.get("INVOKE_QUEUE")
DLQ_URL = environ.get("QUITERMERGE_REPAIRORDER_DLQ_URL")


def send_to_queue(queue_url, dealer_id, end_dt_str):
    """QuiterMergeRepairOrder queue (along with DLQ) that will receive notifications from InvokeLambda"""
    data = {
        "dealer_id": dealer_id,
        "end_dt_str": end_dt_str,
    }
    logger.info(f"Sending {data} to {queue_url}")

    try:
        SQS_CLIENT.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(data),
            DelaySeconds=random.randint(0, 600),
        )
    except Exception as e:
        logger.error(f"Failed to send message to {queue_url}: {e}")
        raise


def send_to_dlq(data):
    """Handle DLQ messages by sending them to the QuiterMergeRepairOrderDLQ queue."""
    try:
        logger.info(f"Sending message to DLQ: {DLQ_URL}")
        SQS_CLIENT.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=dumps(data),
        )
    except Exception as e:
        logger.error(f"Failed to send message to DLQ: {e}")
        raise
