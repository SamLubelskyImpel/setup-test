import logging
from os import environ
from datetime import datetime
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

        consumer.first_name = body["first_name"]
        consumer.last_name = body["last_name"]
        consumer.middle_name = body["middle_name"]
        consumer.email = body["email"]
        consumer.phone = body["phone"]
        consumer.email_optin_flag = body["email_optin_flag"]
        consumer.sms_optin_flag = body["sms_optin_flag"]
        consumer.city = body["city"]
        consumer.country = body["country"]
        consumer.address = body["address"]
        consumer.postal_code = body["postal_code"]
        # consumer.db_update_date = datetime.utcnow()

        session.commit()

    logger.info(f"Consumer updated {consumer_id}")

    return {
        "statusCode": "200"
    }
