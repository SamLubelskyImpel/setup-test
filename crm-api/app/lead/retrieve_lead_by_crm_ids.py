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
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
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
    """Retrieve lead by crm_lead_id. crm_dealer_id and integration_partner_name."""
    logger.info(f"Event: {event}")

    try:
        crm_lead_id = event["pathParameters"]["crm_lead_id"]
        crm_dealer_id = event["queryStringParameters"]["crm_dealer_id"]
        integration_partner_name = event["queryStringParameters"]["integration_partner_name"]

        with DBSession() as session:
            dealer_partner = session.query(DealerIntegrationPartner).join(DealerIntegrationPartner.integration_partner).filter(
                DealerIntegrationPartner.crm_dealer_id == crm_dealer_id,
                DealerIntegrationPartner.is_active == True,
                IntegrationPartner.impel_integration_partner_name == integration_partner_name
            ).first()

            if not dealer_partner:
                logger.error(f"No active dealer found with id {crm_dealer_id}.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {crm_dealer_id}."})
                }

            leads = session.query(Lead).join(Lead.consumer).join(Consumer.dealer_integration_partner).filter(
                Lead.crm_lead_id == crm_lead_id,
                DealerIntegrationPartner.id == dealer_partner.id
            ).all()

            if not leads:
                logger.error(f"Lead with crm_lead_id: {crm_lead_id} and crm_dealer_id: {crm_dealer_id} not found.")
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": f"Lead with crm_lead_id: {crm_lead_id} and crm_dealer_id: {crm_dealer_id} not found."})
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
                "trade_in_vin": vehicle.trade_in_vin,
                "trade_in_year": vehicle.trade_in_year,
                "trade_in_make": vehicle.trade_in_make,
                "trade_in_model": vehicle.trade_in_model,
                "db_creation_date": vehicle.db_creation_date
            }
            vehicles.append(vehicle_record)
            logger.info(f"Found vehicle {vehicle.as_dict()}")

        lead_record = {
            "lead_id": lead.id,
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
