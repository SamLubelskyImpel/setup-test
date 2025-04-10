"""Retrieve dealers."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from sqlalchemy import or_
from uuid import uuid4
from utils import send_alert_notification, model_to_dict
from datetime import datetime, timezone


from inventory_orm.models.inv_vehicle import InvVehicle
from inventory_orm.models.inv_inventory import InvInventory
from inventory_orm.models.inv_dealer_integration_partner import InvDealerIntegrationPartner
from inventory_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve inventory items from Inventory DB"""
    
    request_id = str(uuid4())
    dealer_id = None
    vin = None
    on_lot = None
    max_results = 1000
    query_params = event.get("queryStringParameters", {})

    page = 1 if not query_params else int(query_params.get("page", "1"))
    result_count = (
        max_results
        if not query_params
        else int(query_params.get("result_count", max_results))
    )
    max_results = min(max_results, result_count)

    logger.info(f"Request ID: {request_id}")
    logger.info(f"Event: {event}")
    logger.info(f"Page: {page}")

    try:
        inventory_items = []

        if query_params:
            dealer_id = query_params.get("dealer_id")
            vin = query_params.get("vin", None)
            on_lot = query_params.get("on_lot", 'false') == "true"

        logger.info(f"Query parameters: {query_params}")

        with DBSession() as session:
            db_results = session.query(
                InvInventory, InvVehicle
            ).join(
                InvVehicle, InvInventory.vehicle_id == InvVehicle.id
            ).join(
                InvDealerIntegrationPartner, InvInventory.dealer_integration_partner_id == InvDealerIntegrationPartner.id
            ).filter(
                InvDealerIntegrationPartner.dealer_id == dealer_id
            )

            if vin:
                logger.info(f"Filtering by VIN: {vin}")
                db_results = db_results.filter(
                    InvVehicle.vin == vin
                )

            if on_lot:
                logger.info(f"Filtering by on_lot: {on_lot}")
                db_results = db_results.filter(
                    InvInventory.on_lot.is_(on_lot)
                )

            total_count = db_results.count()
            logger.info(f"Total count of inventory items: {total_count}")
            
            db_results = db_results.limit(max_results + 1).offset((page - 1) * max_results).all()

            if not db_results:
                error_msg = f"No inventory items found for query parameters: {query_params}"
                logger.error(error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps(
                            {
                                "received_date_utc": datetime.now(timezone.utc).isoformat(),
                                "has_next_page": False,
                                "total_count": 0,
                                "results": [],
                            },
                    )
                }

            logger.info(f"Found {len(db_results)} inventory items for query parameters: {query_params}")

            for inventory_db, vehicle_db in db_results[:max_results]:
                inventory_data = model_to_dict(inventory_db, rename_id_to="inv_inventory_id")
                vehicle_data = model_to_dict(vehicle_db, rename_id_to="inv_vehicle_id")

                inventory_record = {
                    **inventory_data,
                    **vehicle_data
                }

                inventory_items.append(inventory_record)


        return {
            "statusCode": 200,
            "body": dumps(
                {
                    "received_date_utc": datetime.now(timezone.utc).isoformat(),
                    "has_next_page": len(db_results) > max_results,
                    "total_count": total_count,
                    "results": inventory_items,
                },
            ),
        }


    except Exception as e:
        logger.error(f"Error retrieving inventory: {str(e)}")

        send_alert_notification(request_id=request_id, endpoint="retrieve_inventory", e=e)

        return {
            "statusCode": 500,
            "body": dumps({"error": f"An error occurred while processing the request. - {str(e)}"})
        }
