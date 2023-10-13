"""Retrieve leads from the shared CRM layer."""
import logging
import json
from os import environ
from decimal import Decimal
from collections import defaultdict
from sqlalchemy.orm import joinedload
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class DecimalEncoder(json.JSONEncoder):
    """JSONEncoder that turns Decimal objects into strings."""

    def default(self, obj: object) -> object:
        """Return string if obj is Decimal, else calls superclass method."""
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve a list of all leads for a given integration partner id."""
    logger.info(f"Event: {event}")
    try:
        filters = event.get("queryStringParameters", {})
        dealer_id = filters.get("dealer_id", None)
        db_creation_date_start = filters["db_creation_date_start"]
        db_creation_date_end = filters["db_creation_date_end"]

        if dealer_id:
            with DBSession() as session:
                dealer = session.query(
                    Dealer
                ).filter(
                    Dealer.product_dealer_id == dealer_id
                ).first()

                if not dealer:
                    logger.error(f"Dealer not found {dealer_id}")
                    return {
                        "statusCode": "404"
                    }

                impel_dealer_id = dealer.id

        # Validate that end date is after start date
        if db_creation_date_end <= db_creation_date_start:
            logger.error("End date must be after start date")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "End date must be after start date"})
            }

        page = 1 if not filters else int(filters.get("page", "1"))
        max_results = 100
        result_count = (
            max_results
            if not filters
            else int(filters.get("result_count", max_results))
        )
        max_results = min(max_results, result_count)

        leads = []  # List to hold all the leads across all pages

        while True:  # Loop through pages
            with DBSession() as session:
                leads_query = (
                    session.query(Lead)
                    .join(Consumer, Lead.consumer_id == Consumer.id)
                    .options(joinedload(Lead.vehicles))
                    .filter(
                        Lead.db_creation_date >= db_creation_date_start,
                        Lead.db_creation_date <= db_creation_date_end
                    )
                )

                if dealer_id:
                    leads_query = leads_query.filter(Consumer.dealer_id == impel_dealer_id)

                leads_page = (
                    leads_query.order_by(Lead.db_creation_date)
                    .limit(max_results)
                    .offset((page - 1) * max_results)
                    .all()
                )

                if not leads_page:
                    break  # Exit the loop if there are no more leads

                lead_ids = [lead.id for lead in leads_page]
                vehicles_db = (
                    session.query(Vehicle)
                    .filter(Vehicle.lead_id.in_(lead_ids))
                    .all()
                )

            # Organize vehicles by lead_id
            vehicles_by_lead = defaultdict(list)

            for vehicle in vehicles_db:
                vehicle_record = {
                    "vin": vehicle.vin,
                    "type": vehicle.type,
                    "vehicle_class": vehicle.vehicle_class,
                    "mileage": vehicle.mileage,
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
                vehicles_by_lead[vehicle.lead_id].append(vehicle_record)

            # Build lead records for the current page
            for lead in leads_page:
                lead_record = {
                    "consumer_id": lead.consumer_id,
                    "salesperson_id": lead.salesperson_id,
                    "lead_status": lead.status,
                    "lead_substatus": lead.substatus,
                    "lead_comment": lead.comment,
                    "lead_origin": lead.origin_channel,
                    "lead_source": lead.source_channel,
                    "vehicles_of_interest": vehicles_by_lead[lead.id]
                }
                leads.append(lead_record)

            page += 1

        return {
            "statusCode": 200,
            "body": json.dumps(leads, cls=DecimalEncoder)
        }

    except Exception as e:
        logger.exception(f"Error retrieving leads: {e}.")
        raise
