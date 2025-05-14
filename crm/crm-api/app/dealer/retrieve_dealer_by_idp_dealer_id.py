"""Retrieve dealer by id."""

import logging
from os import environ
from json import dumps
from typing import Any
from sqlalchemy import or_

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
        idp_dealer_id = event["pathParameters"]["idp_dealer_id"]

        with DBSession() as session:
            db_results = session.query(
                DealerIntegrationPartner, Dealer, IntegrationPartner.impel_integration_partner_name
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Dealer.idp_dealer_id == idp_dealer_id,
                or_(DealerIntegrationPartner.is_active.is_(True),
                    DealerIntegrationPartner.is_active_salesai.is_(True),
                    DealerIntegrationPartner.is_active_chatai.is_(True))
            ).first()

            if not db_results:
                logger.error(f"No active dealer integration found with idp_dealer_id: {idp_dealer_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer partner found with idp_dealer_id: {idp_dealer_id}"})
                }

            dip_db, dealer_db, partner_name = db_results

            logger.info(f"Found dealer integration: {dip_db.as_dict()}")

            if dealer_db.metadata_:
                timezone = dealer_db.metadata_.get("timezone", "")
            else:
                logger.warning(f"No metadata found for dealer: {idp_dealer_id}")
                timezone = ""

            dealer_record = {
                "product_dealer_id": dealer_db.product_dealer_id,
                "idp_dealer_id": dealer_db.idp_dealer_id,
                "dealer_integration_partner_id": dip_db.id,
                "dealer_name": dealer_db.dealer_name,
                "timezone": timezone,
                "integration_partner_name": partner_name,
                # Activation flags
                "is_active": dip_db.is_active,
                "is_active_salesai": dip_db.is_active_salesai,
                "is_active_chatai": dip_db.is_active_chatai,
                "metadata": dip_db.metadata_
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
