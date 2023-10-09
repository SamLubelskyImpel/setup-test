import logging
from os import environ
from json import loads

from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Update consumer."""
    logger.info(f"Event: {event}")

    body = loads(event["body"])
    consumer_id = event["pathParameters"]["consumer_id"]

    fields_to_update = [
        "first_name", "last_name", "middle_name", "email", "phone",
        "email_optin_flag", "sms_optin_flag", "city", "country",
        "address", "postal_code"
    ]

    with DBSession() as session:
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

        for field in fields_to_update:
            if field in body:
                setattr(consumer, field, body[field])

        session.commit()

    logger.info(f"Consumer updated {consumer_id}")

    return {
        "statusCode": "200"
    }
