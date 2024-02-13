"""Retrieve dealer by id."""

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
    """Retrieve dealer by id."""
    logger.info(f"Event: {event}")

    try:
        product_dealer_id = event["pathParameters"]["dealer_id"]

        with DBSession() as session:
            crm_dealer = session.query(
                    Dealer
                ).filter(
                    Dealer.product_dealer_id == product_dealer_id
                ).first()
            if not crm_dealer:
                logger.error(f"No dealer found with dealer_id: {product_dealer_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No dealer found with dealer_id: {product_dealer_id}"})
                }
            logger.info(f"Found dealer: {crm_dealer.as_dict()}")

            crm_dealer_partner = session.query(
                    DealerIntegrationPartner
                ).filter(
                    DealerIntegrationPartner.dealer_id == crm_dealer.id,
                    DealerIntegrationPartner.is_active == True
                ).first()
            if not crm_dealer_partner:
                logger.error(f"No active dealer partner found with dealer_id: {product_dealer_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer partner found with dealer_id: {product_dealer_id}"})
                }

            logger.info(f"Found dealer partner: {crm_dealer_partner.as_dict()}")

            metadata = crm_dealer.metadata_
            if metadata:
                timezone = metadata.get("timezone", "")
            else:
                logger.warning(f"No metadata found for dealer: {product_dealer_id}")
                timezone = ""

            dealer_record = {
                "product_dealer_id": crm_dealer.product_dealer_id,
                "dealer_integration_partner_id": crm_dealer_partner.id,
                "dealer_name": crm_dealer.dealer_name,
                "timezone": timezone,
                "integration_partner_name": crm_dealer_partner.integration_partner.impel_integration_partner_name
            }

        return {
            "statusCode": "200",
            "body": dumps(dealer_record)
        }

    except Exception as e:
        logger.error(f"Error retrieving dealer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
