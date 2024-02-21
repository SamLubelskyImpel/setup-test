"""Retrieve consumer by crm ids."""

import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve consumer."""
    logger.info(f"Event: {event}")

    try:
        crm_consumer_id = event["pathParameters"]["crm_consumer_id"]
        crm_dealer_id = event["queryStringParameters"]["crm_dealer_id"]
        integration_partner_name = event["queryStringParameters"]["integration_partner_name"]

        with DBSession() as session:
            dealer_partner = session.query(DealerIntegrationPartner).join(DealerIntegrationPartner.integration_partner).filter(
                DealerIntegrationPartner.crm_dealer_id == crm_dealer_id,
                DealerIntegrationPartner.is_active == True,
                IntegrationPartner.impel_integration_partner_name == integration_partner_name
            ).first()

            if not dealer_partner:
                logger.error(f"No active dealer found with id {crm_dealer_id}.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {crm_dealer_id}."})
                }

            consumer = session.query(Consumer).join(Consumer.dealer_integration_partner).filter(
                Consumer.crm_consumer_id == crm_consumer_id,
                DealerIntegrationPartner.id == dealer_partner.id
            ).first()

            if not consumer:
                message = f"Consumer with crm_consumer_id: {crm_consumer_id} for crm_dealer_id: {crm_dealer_id} not found."
                logger.error(message)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": message})
                }

            logger.info(f"Found consumer {consumer.as_dict()}")

            consumer_record = {
                "dealer_id": dealer_partner.dealer.product_dealer_id,
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
