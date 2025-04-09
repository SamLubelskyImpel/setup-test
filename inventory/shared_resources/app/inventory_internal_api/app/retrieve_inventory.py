"""Retrieve dealers."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from sqlalchemy import or_
from uuid import uuid4
from utils import send_alert_notification

from inventory_orm.models.inv_vehicle import InvVehicle
from inventory_orm.models.inv_inventory import InvInventory
from inventory_orm.models.inv_dealer_integration_partner import InvDealerIntegrationPartner
from inventory_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve inventory items from Inventory DB"""
    
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    logger.info(f"Event: {event}")

    try:
        inventory_items = []
        query_params = event.get("queryStringParameters", {})
        dealer_id = query_params.get("dealer_id", None)
        vin = query_params.get("vin", None)
        on_lot = query_params.get("on_lot", False)

        logger.info(f"Query parameters: {query_params}")

        with DBSession() as session:
            db_results = session.query(
                InvInventory, InvVehicle
            ).join(
                InvVehicle, InvInventory.vehicle_id == InvVehicle.id
            )

            if dealer_id:
                db_results = db_results.join(
                    InvDealerIntegrationPartner, InvInventory.dealer_integration_partner_id == InvDealerIntegrationPartner.id
                ).filter(
                    InvDealerIntegrationPartner.dealer_id == dealer_id
                )

            if vin:
                db_results = db_results.filter(
                    InvVehicle.vin == vin
                )

            if on_lot:
                db_results = db_results.filter(
                    InvInventory.on_lot.is_(on_lot)
                )

            db_results = db_results.all()

            if not db_results:
                error_msg = f"No inventory items found for query parameters: {query_params}"
                logger.error(error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }

            logger.info(f"Found {len(db_results)} inventory items for query parameters: {query_params}")

            for inventory_db, vehicle_db in db_results:
                inventory_data = inventory_db.as_dict_custom()
                vehicle_data = vehicle_db.as_dict_custom()

                inventory_record = {
                    **inventory_data,
                    **vehicle_data
                }

                inventory_items.append(inventory_record)

        return {
            "statusCode": 200,
            "body": dumps(inventory_items)
        }


    except Exception as e:
        send_alert_notification(request_id=request_id, endpoint="retrieve_inventory", e=e)

        logger.error(f"Error retrieving dealers: {str(e)}")

        return {
            "statusCode": 500,
            "body": dumps({"error": f"An error occurred while processing the request. - {str(e)}"})
        }
