"""Retrieve leads from the shared CRM layer."""
import logging
import json
from math import ceil
from json import dumps
from os import environ
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from sqlalchemy.orm import joinedload
from typing import Any, Optional, List, Dict

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer
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


def get_dealer_id(dealer_id: str) -> Any:
    """Get the impel dealer id based on the product dealer id."""
    with DBSession() as session:
        dealer = session.query(Dealer).filter(Dealer.product_dealer_id == dealer_id).first()

        if not dealer:
            logger.error(f"Dealer not found {dealer_id}")
            return None

        return dealer.id


def retrieve_leads_from_db(
    start_date: str,
    end_date: str,
    page: int,
    max_results: int,
    dealer_id: Optional[int] = None
) -> Any:
    """Retrieve leads from the database."""
    leads = []

    with DBSession() as session:
        leads_query = (
            session.query(Lead)
            .join(Consumer, Lead.consumer_id == Consumer.id)
            .options(joinedload(Lead.vehicles))
            .filter(
                Lead.db_creation_date >= start_date,
                Lead.db_creation_date <= end_date
            )
        )

        if dealer_id:
            leads_query = leads_query.filter(Consumer.dealer_id == dealer_id)

        leads_page = (
            leads_query.order_by(Lead.db_creation_date)
            .limit(max_results)
            .offset((page - 1) * max_results)
            .all()
        )

        total_records = leads_query.count()
        records_on_page = len(leads_page)
        total_pages = ceil(total_records / max_results)

        leads.extend(build_lead_records(leads_page, session))

    return {
        "pagination": {
            "records_on_page": records_on_page,
            "total_records": total_records,
            "total_pages": total_pages,
            "current_page": page
        },
        "leads": leads
    }


def build_lead_records(leads_page: List[Lead], session: Any) -> List[Dict[str, Any]]:
    """Build lead records for the current page."""
    leads = []
    lead_ids = [lead.id for lead in leads_page]
    vehicles_db = (
        session.query(Vehicle)
        .filter(Vehicle.lead_id.in_(lead_ids))
        .all()
    )

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
            "vehicle_comments": vehicle.vehicle_comments,
            "db_creation_date": vehicle.db_creation_date
        }

        vehicles_by_lead[vehicle.lead_id].append(vehicle_record)

    for lead in leads_page:
        vehicles_of_interest_sorted = sorted(
            vehicles_by_lead[lead.id],
            key=lambda x: x["db_creation_date"],  # type: ignore
            reverse=True
        )
        lead_record = {
            "lead_id": lead.id,
            "consumer_id": lead.consumer_id,
            "lead_status": lead.status,
            "lead_substatus": lead.substatus,
            "lead_comment": lead.comment,
            "lead_origin": lead.origin_channel,
            "lead_source": lead.source_channel,
            "vehicles_of_interest": vehicles_of_interest_sorted
        }
        leads.append(lead_record)

    return leads


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve a list of all leads for a given integration partner id."""
    logger.info(f"Event: {event}")
    try:
        filters = event.get("queryStringParameters", {})
        page = int(filters.get("page", 1))
        max_results = min(1000, int(filters.get("result_count", 1000)))
        dealer_id = filters.get("dealer_id", None)
        db_creation_date_start = filters["db_creation_date_start"]
        db_creation_date_end = filters["db_creation_date_end"]

        # Validate that end date is after start date
        if db_creation_date_end <= db_creation_date_start:
            logger.error("End date must be after start date")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "End date must be after start date"})
            }

        impel_dealer_id = None
        if dealer_id:
            impel_dealer_id = get_dealer_id(dealer_id)
            if not impel_dealer_id:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": f"Dealer not found {dealer_id}"})
                }

        leads = retrieve_leads_from_db(
                db_creation_date_start,
                db_creation_date_end,
                page,
                max_results,
                impel_dealer_id
        )

        return {
            "statusCode": 200,
            "body": dumps(leads, cls=CustomEncoder)
        }

    except Exception as e:
        logger.exception(f"Error retrieving leads: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
