"""Retrieve consumer."""

import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession
from crm_orm.models.dealer import Dealer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner

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
            consumer_query = session.query(
                Consumer, Dealer.product_dealer_id
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Consumer.id == consumer_id
            )

            consumer_query = get_restricted_query(consumer_query, integration_partner)
            db_results = consumer_query.first()

            if not db_results:
                logger.error(f"Consumer {consumer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Consumer {consumer_id} not found."})
                }

            consumer_db, product_dealer_id = db_results

            logger.info(f"Found consumer {consumer_db.as_dict()}")

            consumer_record = {
                "dealer_id": product_dealer_id,
                "crm_consumer_id": consumer_db.crm_consumer_id,
                "first_name": consumer_db.first_name,
                "last_name": consumer_db.last_name,
                "middle_name": consumer_db.middle_name,
                "email": consumer_db.email,
                "phone": consumer_db.phone,
                "email_optin_flag": consumer_db.email_optin_flag,
                "sms_optin_flag": consumer_db.sms_optin_flag,
                "city": consumer_db.city,
                "country": consumer_db.country,
                "address": consumer_db.address,
                "postal_code": consumer_db.postal_code
            }

        return {
            "statusCode": 200,
            "body": dumps(consumer_record)
        }

    except Exception as e:
        logger.error(f"Error retrieving consumer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
