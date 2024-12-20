"""Retrieve dealers."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from sqlalchemy import or_

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
        integration_partner_name = event["queryStringParameters"]["integration_partner_name"].split('|')

        with DBSession() as session:
            db_results = session.query(
                DealerIntegrationPartner, Dealer
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                IntegrationPartner.impel_integration_partner_name.in_(integration_partner_name),
                or_(DealerIntegrationPartner.is_active.is_(True),
                    DealerIntegrationPartner.is_active_salesai.is_(True),
                    DealerIntegrationPartner.is_active_chatai.is_(True))
            ).all()

            if not db_results:
                logger.error(f"No active dealers found for {integration_partner_name}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealers found for {integration_partner_name}"})
                }

            logger.info(f"Found {len(db_results)} active dealers for {integration_partner_name}")

            for dip_db, dealer_db in db_results:
                dealer_record = {
                    "dealer_integration_partner_id": dip_db.id,
                    "crm_dealer_id": dip_db.crm_dealer_id,
                    "product_dealer_id": dealer_db.product_dealer_id,
                    "dealer_name": dealer_db.dealer_name,
                    # Activation flags
                    "is_active": dip_db.is_active,
                    "is_active_salesai": dip_db.is_active_salesai,
                    "is_active_chatai": dip_db.is_active_chatai,
                    "metadata": dip_db.metadata_
                }

                if dealer_db.metadata_:
                    dealer_record["metadata"] = dealer_db.metadata_

                dealer_records.append(dealer_record)

        return {
            "statusCode": 200,
            "body": dumps(dealer_records)
        }

    except Exception as e:
        logger.error(f"Error retrieving dealers: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
