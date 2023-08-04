"""Tekion repair order API call."""
import logging
from os import environ
from json import loads
from datetime import datetime, timezone
from uuid import uuid4
from app.tekion_wrapper import TekionWrapper

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def parse_data(data):
    """Parse and handle SQS Message."""
    tekion_wrapper = TekionWrapper(
        dealer_id=data["dealer_id"],
    )

    api_data = tekion_wrapper.get_repair_orders()
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    filename = f'{data["dealer_id"]}_{str(uuid4())}.json'
    tekion_wrapper.upload_data(
        api_data, f'tekion/repair_order/{now.year}/{now.month}/{now.day}/{filename}'
    )

def lambda_handler(event, context):
    """Query Tekion repair order API."""
    try:
        for event in [e for e in event["Records"]]:
            parse_data(loads(event["body"]))
    except Exception:
        logger.exception("Error running repair order lambda")
        raise
