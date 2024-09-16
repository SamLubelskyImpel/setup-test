"""Invoke every day and take all active dealers from quiter to pull data from ftp server."""
import random
import logging
from datetime import datetime, timezone
from json import dumps
from os import environ

import boto3
from dms_api_wrapper import DMSApiWrapper

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
SQS_CLIENT = boto3.client("sqs")
INVOKE_QUEUE = environ.get("INVOKE_QUEUE")


def send_to_queue(queue_url, dealer_id, end_dt_str):
    """Call SQS queue to invoke API call for specific dealer."""
    data = {
        "dealer_id": dealer_id,
        "end_dt_str": end_dt_str,
    }
    logger.info(f"Sending {data} to {queue_url}")
    SQS_CLIENT.send_message(
        QueueUrl=queue_url,
        MessageBody=dumps(data)
    )


def lambda_handler(event, context):
    """Invoke Scheduled Quiter."""
    try:
        end_dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        dms_wrapper = DMSApiWrapper()
        all_dealer_info = dms_wrapper.get_integration_dealers("quiter")
        for dealer_info in all_dealer_info:
            if dealer_info["is_active"]:
                send_to_queue(
                    INVOKE_QUEUE,
                    dealer_info["dms_id"],
                    end_dt_str,
                )
    except Exception:
        logger.exception(f"Error invoking quiter {event}")
        raise