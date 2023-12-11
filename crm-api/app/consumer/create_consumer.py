"""Create consumer."""

import logging
from os import environ
from json import dumps, loads
from typing import Any

from crm_orm.models.dealer import Dealer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
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
        product_dealer_id = event["queryStringParameters"]["dealer_id"]

        with DBSession() as session:
            dealer_partner = session.query(DealerIntegrationPartner).\
                join(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id).\
                filter(
                    Dealer.product_dealer_id == product_dealer_id,
                    DealerIntegrationPartner.is_active == True
                ).first()
            if not dealer_partner:
                logger.error(f"No active dealer found with id {product_dealer_id}. Consumer failed to be created.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {product_dealer_id}. Consumer failed to be created."})
                }

            consumer = Consumer(
                dealer_integration_partner_id=dealer_partner.id,
                first_name=body["first_name"],
                last_name=body["last_name"],
                middle_name=body.get("middle_name"),
                email=body["email"],
                phone=body.get("phone"),
                postal_code=body.get("postal_code"),
                address=body.get("address"),
                country=body.get("country"),
                city=body.get("city"),
                email_optin_flag=body.get("email_optin_flag"),
                sms_optin_flag=body.get("sms_optin_flag"),
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
