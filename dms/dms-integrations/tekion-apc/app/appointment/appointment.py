"""Tekion appointment API call."""
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

    api_data = tekion_wrapper.get_appointments_v3()

    for element in api_data:
        element.update({"dms_id": tekion_wrapper.dealer_id})
    now = tekion_wrapper.end_dt
    filename = f'{data["dealer_id"]}_{str(uuid4())}.json'
    key = f'tekion-apc/service_appointment/{now.year}/{now.month}/{now.day}/{filename}'
    tekion_wrapper.upload_data(
        api_data, key
    )


def lambda_handler(event, context):
    """Query Tekion appointment API."""
    try:
        for record in event["Records"]:
            parse_data(loads(record["body"]))
    except Exception:
        logger.exception("Error running appointment lambda")
        raise
