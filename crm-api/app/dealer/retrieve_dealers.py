"""Retrieve dealers."""

import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve active dealers."""
    logger.info(f"Event: {event}")

    try:
        dealer_records = []
        integration_partner_name = event["queryStringParameters"]["integration_partner_name"]

        with DBSession() as session:
            crm_partner = session.query(
                    IntegrationPartner
                ).filter(
                    IntegrationPartner.impel_integration_partner_name == integration_partner_name
                ).first()

            if not crm_partner:
                logger.error(f"Integration Partner not found {integration_partner_name}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Integration Partner not found {integration_partner_name}"})
                }

            dealer_partners = session.query(
                    DealerIntegrationPartner
                ).filter(
                    Dealer.integration_partner_id == crm_partner.id,
                    Dealer.is_active == True
                ).all()

            logger.info(f"Found {len(dealer_partners)} active dealers for {integration_partner_name}")

            for dealer_partner in dealer_partners:
                dealer_record = {
                    "dealer_integration_partner_id": dealer_partner.id,
                    "crm_dealer_id": dealer_partner.crm_dealer_id,
                    "product_dealer_id": dealer_partner.dealer.product_dealer_id,
                    "dealer_name": dealer_partner.dealer.dealer_name
                }
                dealer_records.append(dealer_record)

        return {
            "statusCode": "200",
            "body": dumps(dealer_records)
        }

    except Exception as e:
        logger.error(f"Error retrieving dealers: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
