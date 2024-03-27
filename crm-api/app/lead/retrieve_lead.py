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
        lead_id = event["pathParameters"]["lead_id"]

        with DBSession() as session:
            lead = session.query(Lead).filter(Lead.id == lead_id).first()
            vehicles_db = (
                session.query(Vehicle)
                .filter(Vehicle.lead_id == lead_id)
                .order_by(desc(Vehicle.db_creation_date))
                .all()
            )

        if not lead:
            logger.error(f"Lead not found {lead_id}")
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Lead not found {lead_id}"})
            }

        logger.info(f"Found lead {lead.as_dict()}")

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
                "db_creation_date": vehicle.db_creation_date
            }
            vehicles.append(vehicle_record)
            logger.info(f"Found vehicle {vehicle.as_dict()}")

        lead_record = {
            "consumer_id": lead.consumer_id,
            "lead_status": lead.status,
            "lead_substatus": lead.substatus,
            "lead_comment": lead.comment,
            "lead_origin": lead.origin_channel,
            "lead_source": lead.source_channel,
            "lead_source_detail": lead.source_detail,
            "vehicles_of_interest": vehicles,
            "lead_ts": lead.lead_ts,
            "db_creation_date": lead.db_creation_date
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
