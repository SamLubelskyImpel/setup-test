import logging
import json
from os import environ
from decimal import Decimal

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


def lambda_handler(event, context):
    """Retrieve lead."""
    logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    with DBSession() as session:
        lead = session.query(Lead).filter(Lead.id == lead_id).first()
        vehicles_db = session.query(Vehicle).filter(Vehicle.lead_id == lead_id).all()

    if not lead:
        logger.error(f"Lead not found {lead_id}")
        return {
            "statusCode": 404
        }

    logger.info(f"Found lead {lead.as_dict()}")

    vehicles = []
    for vehicle in vehicles_db:
        vehicle_record = {
            "vin": vehicle.vin,
            "type": vehicle.type,
            "vehicle_class": vehicle.vehicle_class,
            "mileage": vehicle.type,
            "manufactured_year": vehicle.manufactured_year,
            "make": vehicle.make,
            "model": vehicle.model,
            "trim": vehicle.trim,
            "body_style": vehicle.body_style,
            "transmission": vehicle.transmission,
            "interior_color": vehicle.interior_color,
            "exterior_color": vehicle.exterior_color,
            "price": vehicle.price,
            "status": vehicle.status,
            "condition": vehicle.condition,
            "odometer_units": vehicle.odometer_units,
            "vehicle_comments": vehicle.vehicle_comments
        }
        vehicles.append(vehicle_record)
        logger.info(f"Found vehicle {vehicle.as_dict()}")

    lead_record = {
        "consumer_id": lead.consumer_id,
        "salesperson_id": lead.salesperson_id,
        "lead_status": lead.status,
        "lead_substatus": lead.substatus,
        "lead_comment": lead.comment,
        "lead_origin": lead.origin_channel,
        "lead_source": lead.source_channel,
        "vehicles_of_interest": vehicles
    }

    return {
        "statusCode": 200,
        "body": json.dumps(lead_record, cls=DecimalEncoder)
    }
