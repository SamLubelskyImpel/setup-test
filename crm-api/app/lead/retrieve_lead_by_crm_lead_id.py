"""Retrieve lead by crm_lead_id from the shared CRM layer."""
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
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
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
    """Retrieve lead by crm_lead_id."""
    logger.info(f"Event: {event}")

    try:
        crm_lead_id = event["pathParameters"]["crm_lead_id"]
        crm_dealer_id = event["queryStringParameters"]["crm_dealer_id"]

        logger.info(f"crm_lead_id: {crm_lead_id}")
        logger.info(f"crm_dealer_id: {crm_dealer_id}")

        with DBSession() as session:
            dealer_partner = session.query(DealerIntegrationPartner).filter(
                DealerIntegrationPartner.crm_dealer_id == crm_dealer_id,
                DealerIntegrationPartner.is_active == True
            ).first()
            
            if not dealer_partner:
                logger.error(f"No active dealer found with id {crm_dealer_id}.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {crm_dealer_id}."})
                }

            leads = session.query(Lead).filter(
                Lead.crm_lead_id == crm_lead_id
            ).all()


            leads = [lead for lead in leads if lead.consumer.dealer_integration_partner.id == dealer_partner.id]

            if not leads:
                logger.error(f"Lead not found {crm_lead_id}")
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": f"Lead not found {crm_lead_id}"})
                }

            # Check if multiple leads are found
            if len(leads) > 1:
                logger.error(f"Expected one lead but found {len(leads)} with the same crm_lead_id {crm_lead_id}")
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Expected one lead but found {len(leads)} with the same crm_lead_id {crm_lead_id}"})
                }

            lead = leads[0]
            logger.info(f"Found lead {lead.as_dict()}")

            vehicles_db = (
                session.query(Vehicle)
                .filter(Vehicle.lead_id == lead.id)
                .order_by(desc(Vehicle.db_creation_date))
                .all()
            )

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