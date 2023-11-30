"""Create consumer."""

import logging
from os import environ
from json import dumps, loads
from typing import Any

from crm_orm.models.dealer import Dealer
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Create consumer."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        dealer_id = event["queryStringParameters"]["dealer_id"]

        with DBSession() as session:
            dealer = session.query(Dealer).filter(Dealer.product_dealer_id == dealer_id).first()
            if not dealer:
                logger.error(f"Dealer {dealer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Dealer {dealer_id} not found. Consumer failed to be created."})
                }

            consumer = Consumer(
                dealer_id=dealer.id,
                first_name=body["first_name"],
                last_name=body["last_name"],
                middle_name=body.get("middle_name", None),
                email=body["email"],
                phone=body.get("phone", None),
                postal_code=body.get("postal_code", None),
                address=body.get("address", None),
                country=body.get("country", None),
                city=body.get("city", None),
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

    except Exception as e:
        logger.error(f"Error creating consumer: {str(e)}")
        return {
            "statusCode": "500",
            "body": dumps({"error": "An error occurred while processing the request."})
        }
