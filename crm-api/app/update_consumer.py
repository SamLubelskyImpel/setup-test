import logging
from os import environ
from datetime import datetime
from json import dumps

from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Upadte consumer."""
    logger.info(f"Event: {event}")

    body = event["body"]
    consumer_id = event["pathParameters"]["consumer_id"]

    first_name = body["first_name"]
    last_name = body["last_name"]
    middle_name = body.get("middle_name")
    email = body["email"]
    phone = body.get("phone")
    email_optin_flag = body.get("email_optin_flag")
    sms_optin_flag = body.get("sms_optin_flag")
    city = body.get("city")
    country = body.get("country")
    address = body.get("address")
    postal_code = body.get("postal_code")

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

        consumer.first_name = first_name
        consumer.last_name = last_name
        consumer.middle_name = middle_name if middle_name else consumer.middle_name
        consumer.email = email
        consumer.phone = phone if phone else consumer.phone
        consumer.email_optin_flag = email_optin_flag if email_optin_flag else consumer.email_optin_flag
        consumer.sms_optin_flag = sms_optin_flag if sms_optin_flag else consumer.sms_optin_flag
        consumer.city = city if city else consumer.city
        consumer.country = country if country else consumer.country
        consumer.address = address if address else consumer.address
        consumer.postal_code = postal_code if postal_code else consumer.postal_code
        consumer.db_update_date = datetime.utcnow()

        session.commit()

    logger.info(f"Consumer updated {consumer.id}")

    return {
        "statusCode": "200",
        "body": dumps({"consumerId": consumer.id})
    }
