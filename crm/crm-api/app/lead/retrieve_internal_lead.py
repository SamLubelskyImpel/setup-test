"""Retrieve lead by lead_id from the shared CRM layer."""
import logging
import json
from json import dumps
from os import environ
from datetime import datetime
from decimal import Decimal
from typing import Any
from sqlalchemy import desc

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.session_config import DBSession

from utils import get_internal_restricted_query

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class CustomEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and Decimal objects."""

    def default(self, obj: Any) -> Any:
        """Serialize datetime and Decimal objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        return super(CustomEncoder, self).default(obj)


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve lead."""
    logger.info(f"Event: {event}")

    try:
        integration_partner = event["requestContext"]["authorizer"]["integration_partner"]
        lead_id = event["pathParameters"]["lead_id"]

        with DBSession() as session:
            lead_db, impel_integration_partner_name = (
                get_internal_restricted_query(session, integration_partner)
                .filter(Lead.id == lead_id)
                .first()
            )

            if not lead_db:
                logger.error(f"Lead not found {lead_id}")
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": f"Lead not found {lead_id}"})
                }

            vehicles_db = (
                session.query(Vehicle)
                .filter(Vehicle.lead_id == lead_id)
                .order_by(desc(Vehicle.db_creation_date))
                .all()
            )

        logger.info(f"Found lead {lead_db.as_dict()}")

        vehicles = []
        for vehicle in vehicles_db:
            vehicle_record = {
                "vin": vehicle.vin,
                "stock_number": vehicle.stock_num,
                "type": vehicle.type,
                "class": vehicle.vehicle_class,
                "mileage": vehicle.mileage,
                "year": vehicle.manufactured_year,
                "make": vehicle.make,
                "model": vehicle.model,
                "oem_name": vehicle.oem_name,
                "trim": vehicle.trim,
                "body_style": vehicle.body_style,
                "transmission": vehicle.transmission,
                "interior_color": vehicle.interior_color,
                "exterior_color": vehicle.exterior_color,
                "price": vehicle.price,
                "status": vehicle.status,
                "condition": vehicle.condition,
                "odometer_units": vehicle.odometer_units,
                "vehicle_comments": vehicle.vehicle_comments,
                "trade_in_vin": vehicle.trade_in_vin,
                "trade_in_year": vehicle.trade_in_year,
                "trade_in_make": vehicle.trade_in_make,
                "trade_in_model": vehicle.trade_in_model,
                "metadata": vehicle.metadata_,
                "db_creation_date": vehicle.db_creation_date
            }
            vehicles.append(vehicle_record)
            logger.info(f"Found vehicle {vehicle.as_dict()}")

        lead_record = {
            "consumer_id": lead_db.consumer_id,
            "crm_lead_id": lead_db.crm_lead_id,
            "crm_vendor_name": impel_integration_partner_name if not integration_partner else integration_partner,
            "lead_status": lead_db.status,
            "lead_substatus": lead_db.substatus,
            "lead_comment": lead_db.comment,
            "lead_origin": lead_db.origin_channel,
            "lead_source": lead_db.source_channel,
            "lead_source_detail": lead_db.source_detail,
            "vehicles_of_interest": vehicles,
            "metadata": lead_db.metadata_,
            "lead_ts": lead_db.lead_ts,
            "db_creation_date": lead_db.db_creation_date
        }

        return {
            "statusCode": 200,
            "body": dumps(lead_record, cls=CustomEncoder)
        }

    except Exception as e:
        logger.exception(f"Error retrieving lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
