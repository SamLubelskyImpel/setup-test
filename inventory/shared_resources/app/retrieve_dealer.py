"""Retrieve dealers."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from sqlalchemy import or_
from uuid import uuid4
from utils import send_alert_notification, model_to_dict

from inventory_orm.models.inv_dealer import InvDealer
from inventory_orm.models.inv_dealer_integration_partner import InvDealerIntegrationPartner
from inventory_orm.models.inv_integration_partner import InvIntegrationPartner
from inventory_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve active dealers from Inventory DB"""

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    logger.info(f"Event: {event}")

    try:
        dealer_records = []
        query_params = event.get("queryStringParameters") or {}

        logger.info(f"Query parameters: {query_params}")

        partner_name = query_params.get("impel_integration_partner_name", None)
        provider_dealer_id = query_params.get("provider_dealer_id", None)

        with DBSession() as session:
            db_results = session.query(
                InvDealerIntegrationPartner, InvDealer
            ).join(
                InvDealer, InvDealerIntegrationPartner.dealer_id == InvDealer.id
            ).join(
                InvIntegrationPartner, InvDealerIntegrationPartner.integration_partner_id == InvIntegrationPartner.id
            ).filter(
                InvDealerIntegrationPartner.is_active.is_(True)
            )

            if partner_name:
                db_results = db_results.filter(
                    InvIntegrationPartner.impel_integration_partner_id == partner_name
                )

            if provider_dealer_id:
                db_results = db_results.filter(
                    InvDealerIntegrationPartner.provider_dealer_id == provider_dealer_id
                )

            db_results = db_results.all()

            if not db_results:
                error_msg = f"No active dealers found for query parameters: {query_params}"
                logger.error(error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }

            logger.info(f"Found {len(db_results)} active dealers for query parameters: {query_params}")

            for dip_db, dealer_db in db_results:
                dip_data = model_to_dict(dip_db, rename_id_to="inv_dealer_integration_partner_id")
                dealer_data = model_to_dict(dealer_db, rename_id_to="inv_dealer_id")

                dealer_record = {
                    **dip_data,
                    **dealer_data
                }

                dealer_records.append(dealer_record)

        return {
            "statusCode": 200,
            "body": dumps(dealer_records)
        }

    except Exception as e:
        logger.error(f"Error retrieving dealers: {str(e)}")

        send_alert_notification(request_id=request_id, endpoint="retrieve_dealers", e=e)

        return {
            "statusCode": 500,
            "body": dumps({"error": f"An error occurred while processing the request. - {str(e)}"})
        }
