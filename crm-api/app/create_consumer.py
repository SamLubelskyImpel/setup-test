import logging
from os import environ
from json import dumps, loads

from crm_orm.models.dealer import Dealer
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Create consumer."""
    logger.info(f"Event: {event}")

    body = loads(event["body"])
    request_product = event["headers"]["partner_id"]
    dealer_id = event["headers"]["dealer_id"]

    with DBSession() as session:
        dealer = session.query(
            Dealer
        ).filter(
            Dealer.product_dealer_id == dealer_id
        ).first()
        if not dealer:
            logger.error(f"Dealer not found {dealer_id}")
            return {
                "statusCode": "404",
                "message": f"Dealer not found {dealer_id}"
            }

        consumer = Consumer(
            dealer_id=dealer.id,
            first_name=body["first_name"],
            last_name=body["last_name"],
            middle_name=body.get("middle_name", ""),
            email=body["email"],
            phone=body.get("phone", ""),
            postal_code=body.get("postal_code", ""),
            address=body.get("address", ""),
            country=body.get("country", ""),
            city=body.get("city", ""),
            email_optin_flag=body.get("email_optin_flag", True),
            sms_optin_flag=body.get("sms_optin_flag", True),
            request_product=request_product
        )
        session.add(consumer)
        session.commit()

        consumer_id = consumer.id

    logger.info(f"Created consumer {consumer_id}")

    return {
        "statusCode": "201",
        "body": dumps({"consumer_id": consumer_id})
    }
