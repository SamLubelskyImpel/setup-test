import logging
from os import environ
import boto3
from json import dumps
from rds_instance import RDSInstance
from datetime import datetime, timedelta, timezone

_logger = logging.getLogger(__name__)
_logger.setLevel(environ['LOGLEVEL'])

SQS_CLIENT = boto3.client("sqs")
DATA_PULL_QUEUE = environ.get("DATA_PULL_QUEUE")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"

def send_to_queue(queue_url, dealer, resource, date):
    """Send message to SQS Data Pull Queue."""

    data = {
        "dealer_id": dealer[0],
        "dms_id": dealer[1],
        "resource": resource,
        "date": date
    }
    _logger.info(f"Sending {data} to {queue_url}")

    SQS_CLIENT.send_message(
        QueueUrl=queue_url,
        MessageBody=dumps(data)
    )


def lambda_handler(event, context):
    rds_instance = RDSInstance(IS_PROD)

    _logger.info(f"Invoke data pull started for dealertrack")

    active_dealers = rds_instance.select_db_active_dealer_partners("dealertrack-dms")

    if not active_dealers:
        _logger.info("No active dealers found.")
        return

    _logger.info(f"Active dealers: {active_dealers}")

    date_str = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    for dealer in active_dealers:
        _logger.info(f"Sending data pull request for dealer {dealer[0]} - dms_id {dealer[1]}")

        send_to_queue(DATA_PULL_QUEUE, dealer, "service_appointment", date_str)
        send_to_queue(DATA_PULL_QUEUE, dealer, "fi_closed_deal", date_str)
        send_to_queue(DATA_PULL_QUEUE, dealer, "repair_order", date_str)

    _logger.info(f"Invoke data pull finished successfully for dealertrack")
