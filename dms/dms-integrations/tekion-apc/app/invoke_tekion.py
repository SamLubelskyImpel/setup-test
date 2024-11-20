"""Invoke Scheduled Tekion APC API calls for each dealer."""
import random
import logging
from datetime import datetime
from json import dumps
from os import environ
import boto3
from dms_wrapper import DMSWrapper

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")

APIS = {
    "daily": [
        environ["REPAIR_ORDER_QUEUE"],
        environ["DEALS_QUEUE"],
        environ["HISTORICAL_DATA_QUEUE"],
        environ["APPOINTMENTS_QUEUE"],
    ]
}


def send_to_queue(queue_url, dealer_id, end_dt_str):
    """Call SQS queue to invoke API call for specific dealer."""
    data = {
        "dealer_id": dealer_id,
        "end_dt_str": end_dt_str,
    }
    logger.info(f"Sending {data} to {queue_url}")
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=dumps(data),
        # Spread out calls, avoid overloading Tekion servers.
        DelaySeconds=random.randint(0, 500),
    )


def lambda_handler(event, context):
    """Invoke Scheduled Tekion API calls."""
    try:
        frequency = event["frequency"]
        end_dt_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        dms_wrapper = DMSWrapper()
        all_dealer_info = dms_wrapper.get_integration_dealers("tekion-apc")
        for queue_url in APIS[frequency]:
            for dealer_info in all_dealer_info:
                if dealer_info["is_active"]:
                    send_to_queue(
                        queue_url,
                        dealer_info["dms_id"],
                        end_dt_str,
                    )
    except Exception:
        logger.exception(f"Error invoking tekion apc apis {event}")
        raise
