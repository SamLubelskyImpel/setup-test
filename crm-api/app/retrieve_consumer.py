import logging
from os import environ
from json import dumps

from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Retrive consumer."""
    logger.info(f"Event: {event}")

    consumer_id = event["pathParameters"]["consumer_id"]

    with DBSession as session:
        consumer = session.query(
            Consumer
        ).filter(
            Consumer.id == consumer_id
        ).first()

    if not consumer:
        logger.error(f"Consumer not found {consumer_id}")
        return {
            "statusCode": "404"
        }

    logger.info(f"Found consumer {consumer.as_dict()}")

    consumer_record = {
        "first_name": consumer.first_name,
        "last_name": consumer.last_name,
        "middle_name": consumer.middle_name,
        "email": consumer.email,
        "phone": consumer.phone,
        "email_optin_flag": consumer.email_optin_flag,
        "sms_optin_flag": consumer.sms_optin_flag,
        "city": consumer.city,
        "country": consumer.country,
        "address": consumer.address,
        "postal_code": consumer.postal_code
    }

    return {
        "statusCode": "200",
        "body": dumps(consumer_record)
    }
