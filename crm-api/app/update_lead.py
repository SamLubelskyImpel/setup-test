"""Update lead."""
import logging
from json import loads
from os import environ
from typing import Any
from datetime import datetime

from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.models.vehicle import Vehicle
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Update lead."""
    # logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    body = loads(event["body"])
    consumer_id = int(body["consumer_id"])
    vehicles_of_interest = body['vehicles_of_interest']

    field_mapping = {
        "lead_status": "status",
        "lead_substatus": "substatus",
        "lead_comment": "comment",
        "lead_origin": "origin_channel",
        "lead_source": "source_channel"
    }

    with DBSession() as session:
        lead = session.query(Lead).filter(Lead.id == lead_id).first()

        if not lead:
            logger.error(f"Lead not found {lead_id}")
            return {
                "statusCode": "404"
            }

        consumer = session.query(
            Consumer
        ).filter(
            Consumer.id == consumer_id
        ).first()

        if not consumer:
            logger.error(f"Consumer not found {consumer_id}")
            return {
                "statusCode": "404"
            }

        logger.info('Here!!!')

        # Update fields based on the mapping and log the updates
        for received_field, db_field in field_mapping.items():
            if received_field in body:
                new_value = body[received_field]
                setattr(lead, db_field, new_value)

        lead.consumer_id = consumer_id
        lead.db_update_date = datetime.utcnow()
        lead.db_update_role = "system"

        for vehicle in vehicles_of_interest:
            vehicle = Vehicle(
                lead_id=lead.id,
                vin=vehicle.get("vin", None),
                type=vehicle.get("type", None),
                vehicle_class=vehicle.get("vehicle_class", None),
                mileage=vehicle.get("mileage", None),
                make=vehicle.get("make", None),
                model=vehicle.get("model", ""),
                manufactured_year=vehicle.get("manufactured_year", None),
                body_style=vehicle.get("body_style", None),
                transmission=vehicle.get("transmission", None),
                interior_color=vehicle.get("interior_color", None),
                exterior_color=vehicle.get("exterior_color", None),
                trim=vehicle.get("trim", None),
                price=vehicle.get("price", None),
                status=vehicle.get("status", None),
                condition=vehicle.get("condition", None),
                odometer_units=vehicle.get("odometer_units", None),
                vehicle_comments=vehicle.get("vehicle_comments", None),
                db_creation_date=datetime.utcnow(),
                db_update_date=datetime.utcnow(),
                db_update_role="system",
            )
            lead.vehicles.append(vehicle)

        session.commit()

    logger.info(f"Lead is updated {lead_id}")

    return {
        "statusCode": "200"
    }
