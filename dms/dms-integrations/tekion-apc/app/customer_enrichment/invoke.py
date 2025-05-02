import logging
from os import environ
import boto3
from json import dumps
from dms_wrapper import DMSWrapper


logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())

PULL_CUSTOMERS_QUEUE = environ.get("PULL_CUSTOMERS_QUEUE")

SM = boto3.client("secretsmanager")
SQS = boto3.client("sqs")


def get_active_dealers():
    dms_wrapper = DMSWrapper()
    dealers = dms_wrapper.get_integration_dealers("tekion-apc")
    return [d["dms_id"] for d in dealers if d["is_active"]]


def invoke_handler(event, _):
    try:
        logger.info(f"Event: {event}")
        active_dealers = get_active_dealers()
        for dealer in active_dealers:
            queue_event = { "dms_id": dealer }
            logger.info(f"Sending event {queue_event}")
            SQS.send_message(
                QueueUrl=PULL_CUSTOMERS_QUEUE,
                MessageBody=dumps(queue_event)
            )
    except:
        logger.exception("Failed to invoke customer enrichment")
        raise
