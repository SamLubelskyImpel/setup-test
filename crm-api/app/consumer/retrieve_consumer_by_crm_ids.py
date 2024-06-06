"""Retrieve consumer by crm ids."""

import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.session_config import DBSession
from crm_orm.models.dealer import Dealer

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
            db_results = session.query(
                DealerIntegrationPartner, Dealer
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).filter(
                DealerIntegrationPartner.crm_dealer_id == crm_dealer_id,
                DealerIntegrationPartner.is_active == True,
                IntegrationPartner.impel_integration_partner_name == integration_partner_name
            ).first()

            if not db_results:
                logger.error(f"No active dealer found with id {crm_dealer_id}.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {crm_dealer_id}."})
                }

            dip_db, dealer_db = db_results

            consumer_db = session.query(
                Consumer
            ).filter(
                Consumer.crm_consumer_id == crm_consumer_id,
                DealerIntegrationPartner.id == dip_db.id
            ).first()

            if not consumer_db:
                message = f"Consumer with crm_consumer_id: {crm_consumer_id} for crm_dealer_id: {crm_dealer_id} not found."
                logger.error(message)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": message})
                }

            logger.info(f"Found consumer {consumer_db.as_dict()}")

            consumer_record = {
                "consumer_id": consumer_db.id,
                "dealer_id": dealer_db.product_dealer_id,
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
