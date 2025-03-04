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
from sqlalchemy import or_, desc, asc

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession

from utils import get_restricted_query

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


def get_dealer_partner_id(session, product_dealer_id: str) -> Any:
    """Get the dealer integration partner id based on the product dealer id."""
    dip_db = session.query(
        DealerIntegrationPartner
    ).join(
        Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
    ).filter(
            Dealer.product_dealer_id == product_dealer_id,
            or_(DealerIntegrationPartner.is_active.is_(True),
                DealerIntegrationPartner.is_active_salesai.is_(True),
                DealerIntegrationPartner.is_active_chatai.is_(True))
    ).first()
    if not dip_db:
        logger.error(f"No active dealer found with id {product_dealer_id}.")
        return None

    return dip_db.id


def retrieve_leads_from_db(
    session: Any,
    start_date: str,
    end_date: str,
    page: int,
    max_results: int,
    integration_partner: str,
    sort_order: str,
    dealer_partner_id: Optional[int] = None,
    consumer_id: Optional[int] = None
) -> Any:
    """Retrieve leads from the database."""
    leads = []

    leads_query = get_restricted_query(session, integration_partner)
    leads_query = (
        leads_query.options(joinedload(Lead.vehicles))
        .filter(
            Lead.db_creation_date >= start_date,
            Lead.db_creation_date <= end_date
        )
    )

    if dealer_partner_id and consumer_id:
        leads_query = leads_query.filter(
            Consumer.dealer_integration_partner_id == dealer_partner_id,
            Consumer.id == consumer_id
        )
    elif dealer_partner_id:
        leads_query = leads_query.filter(Consumer.dealer_integration_partner_id == dealer_partner_id)
    elif consumer_id:
        error_message = "A dealer_partner_id is required to filter by consumer_id"
        logger.error(error_message)
        raise ValueError(error_message)

    sort_column = desc(Lead.db_creation_date) if sort_order.lower() == "desc" else asc(Lead.db_creation_date)

    leads_page = (
        leads_query.order_by(sort_column)
        .limit(max_results)
        .offset((page - 1) * max_results)
        .all()
    )

    total_records = leads_query.count()
    records_on_page = len(leads_page)
    total_pages = ceil(total_records / max_results)

    leads.extend(build_lead_records(leads_page, session))

    return {
        "leads": leads,
        "pagination": {
            "records_on_page": records_on_page,
            "total_records": total_records,
            "total_pages": total_pages,
            "current_page": page
        },
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
            "lead_source_detail": lead.source_detail,
            "vehicles_of_interest": vehicles_of_interest_sorted,
            "metadata": lead.metadata_,
            "lead_ts": lead.lead_ts,
            "db_creation_date": lead.db_creation_date
        }
        leads.append(lead_record)

    return leads


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve a list of all leads for a given integration partner id."""
    logger.info(f"Event: {event}")
    try:
        integration_partner = event["requestContext"]["authorizer"]["integration_partner"]
        filters = event.get("queryStringParameters", {})
        page = int(filters.get("page", 1))
        max_results = min(1000, int(filters.get("result_count", 1000)))
        product_dealer_id = filters.get("dealer_id", None)
        consumer_id = int(filters.get("consumer_id")) if filters.get("consumer_id") is not None else None
        db_creation_date_start = filters["db_creation_date_start"]
        db_creation_date_end = filters["db_creation_date_end"]
        sort_order = filters["sort_order"]

        # Validate that end date is after start date
        if db_creation_date_end <= db_creation_date_start:
            logger.error("End date must be after start date")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "End date must be after start date"})
            }

        with DBSession() as session:
            dealer_partner_id = None
            if product_dealer_id:
                dealer_partner_id = get_dealer_partner_id(session, product_dealer_id)
                if not dealer_partner_id:
                    return {
                        "statusCode": 404,
                        "body": json.dumps({"error": f"No active dealer found with id {product_dealer_id}"})
                    }

            leads = retrieve_leads_from_db(
                session,
                db_creation_date_start,
                db_creation_date_end,
                page,
                max_results,
                integration_partner,
                sort_order,
                dealer_partner_id,
                consumer_id
            )

        return {
            "statusCode": 200,
            "body": dumps(leads, cls=CustomEncoder)
        }
    except ValueError as e:
        logger.warning(f"Bad Request: {e}")
        return {
            "statusCode": 400,
            "body": dumps({"error": str(e)})
        }
    except Exception as e:
        logger.exception(f"Error retrieving leads: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
