"""Create lead in the shared CRM layer."""
import logging
from os import environ
from datetime import datetime
from json import dumps, loads
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Create lead."""
    try:
        logger.info(f"Event: {event}")

        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        consumer_id = body["consumer_id"]

        with DBSession() as session:
            consumer = session.query(
                Consumer
            ).filter(
                Consumer.id == consumer_id
            ).first()

            if not consumer:
                logger.error(f"Consumer {consumer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Consumer {consumer_id} not found. Lead failed to be created."})
                }

            # Create lead
            lead = Lead(
                consumer_id=consumer_id,
                status=body["lead_status"],
                substatus=body["lead_substatus"],
                comment=body["lead_comment"],
                origin_channel=body["lead_origin"],
                source_channel=body["lead_source"],
                request_product=request_product,
                lead_ts=datetime.utcnow()
            )

            session.add(lead)

            # Create vehicles of interest
            vehicles_of_interest = body["vehicles_of_interest"]
            for vehicle in vehicles_of_interest:
                vehicle = Vehicle(
                    lead_id=lead.id,
                    vin=vehicle.get("vin"),
                    type=vehicle.get("type"),
                    vehicle_class=vehicle.get("vehicle_class"),
                    mileage=vehicle.get("mileage"),
                    make=vehicle.get("make"),
                    model=vehicle.get("model"),
                    manufactured_year=vehicle.get("manufactured_year"),
                    body_style=vehicle.get("body_style"),
                    transmission=vehicle.get("transmission"),
                    interior_color=vehicle.get("interior_color"),
                    exterior_color=vehicle.get("exterior_color"),
                    trim=vehicle.get("trim"),
                    price=vehicle.get("price"),
                    status=vehicle.get("status"),
                    condition=vehicle.get("condition"),
                    odometer_units=vehicle.get("odometer_units"),
                    vehicle_comments=vehicle.get("vehicle_comments")
                )
                lead.vehicles.append(vehicle)

            session.commit()
            lead_id = lead.id

        logger.info(f"Created lead {lead_id}")

        return {
            "statusCode": "201",
            "body": dumps({"lead_id": lead_id})
        }

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
