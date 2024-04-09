"""Retrieve consumer."""

import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

from utils import get_restricted_query

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve consumer."""
    logger.info(f"Event: {event}")

    try:
        consumer_id = event["pathParameters"]["consumer_id"]
        integration_partner = event["requestContext"]["authorizer"]["integration_partner"]

        with DBSession() as session:
            consumer = (get_restricted_query(session, integration_partner)
                        .filter(Consumer.id == consumer_id)
                        .first())

            if not consumer:
                logger.error(f"Consumer {consumer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Consumer {consumer_id} not found."})
                }

            logger.info(f"Found consumer {consumer.as_dict()}")

            consumer_record = {
                "dealer_id": consumer.dealer_integration_partner.dealer.product_dealer_id,
                "crm_consumer_id": consumer.crm_consumer_id,
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

    except Exception as e:
        logger.error(f"Error retrieving consumer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
