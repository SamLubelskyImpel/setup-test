import logging
from os import environ
from rds_instance import RDSInstance
import boto3
from json import dumps
from datetime import datetime, timezone


DATA_PULL_QUEUE_URL = environ.get("DATA_PULL_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")
rds_instance = RDSInstance()


def lambda_handler(event, _):
    logger.info(f"Event: {event}")

    try:
        current_ts = datetime.now(tz=timezone.utc)

        active_dealers = rds_instance.select_db_active_dealer_partners("icc-api")
        logger.info(f"Active dealers: {active_dealers}")

        for dealer_id, in active_dealers:
            msg = { "provider_dealer_id": dealer_id, "ts": current_ts.isoformat() }
            sqs_client.send_message(
                QueueUrl=DATA_PULL_QUEUE_URL,
                MessageBody=dumps(msg)
            )
    except:
        logger.exception("Error occurred")
        raise
