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
            db_results = session.query(
                DealerIntegrationPartner, Dealer, IntegrationPartner.impel_integration_partner_name
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Dealer.product_dealer_id == product_dealer_id,
                DealerIntegrationPartner.is_active == True
            ).first()

            # crm_dealer = session.query(
            #         Dealer
            #     ).filter(
            #         Dealer.product_dealer_id == product_dealer_id
            #     ).first()
            # if not crm_dealer:
            #     logger.error(f"No dealer found with dealer_id: {product_dealer_id}")
            #     return {
            #         "statusCode": 404,
            #         "body": dumps({"error": f"No dealer found with dealer_id: {product_dealer_id}"})
            #     }
            # logger.info(f"Found dealer: {crm_dealer.as_dict()}")

            # crm_dealer_partner = session.query(
            #         DealerIntegrationPartner
            #     ).filter(
            #         DealerIntegrationPartner.dealer_id == crm_dealer.id,
            #         DealerIntegrationPartner.is_active == True
            #     ).first()
            if not db_results:
                logger.error(f"No active dealer integration found with dealer_id: {product_dealer_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer partner found with dealer_id: {product_dealer_id}"})
                }

            dip_db, dealer_db, partner_name = db_results

            logger.info(f"Found dealer integration: {dip_db.as_dict()}")

            if dealer_db.metadata_:
                timezone = dealer_db.metadata_.get("timezone", "")
            else:
                logger.warning(f"No metadata found for dealer: {product_dealer_id}")
                timezone = ""

            dealer_record = {
                "product_dealer_id": dealer_db.product_dealer_id,
                "dealer_integration_partner_id": dip_db.id,
                "dealer_name": dealer_db.dealer_name,
                "timezone": timezone,
                "integration_partner_name": partner_name
            }

        return {
            "statusCode": 200,
            "body": dumps(dealer_record)
        }

    except Exception as e:
        logger.error(f"Error retrieving dealer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
