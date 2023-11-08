"""Tekion repair order API call."""
import logging
from os import environ
from json import loads
from uuid import uuid4
from tekion_wrapper import TekionWrapper


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def parse_data(data):
    """Parse and handle SQS Message."""
    logger.info(data)

    tekion_wrapper = TekionWrapper(
        dealer_id=data["dealer_id"],
        end_dt_str=data["end_dt_str"]
    )

    api_data = tekion_wrapper.get_repair_orders()

    for element in api_data:
        element.update({"dms_id": tekion_wrapper.dealer_id})
    now = tekion_wrapper.end_dt
    filename = f'{data["dealer_id"]}_{str(uuid4())}.json'
    key = f'tekion/repair_order/{now.year}/{now.month}/{now.day}/{filename}'
    tekion_wrapper.upload_data(
        api_data, key
    )
    logger.info(f"Uploaded {key}")

def lambda_handler(event, context):
    """Query Tekion repair order API."""
    try:
        for event in [e for e in event["Records"]]:
            parse_data(loads(event["body"]))
    except Exception:
        logger.exception("Error running repair order lambda")
        raise
