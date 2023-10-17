"""Retrieve salespersons data from the shared CRM layer."""
import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.salesperson import Salesperson
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_salespersons_for_lead(lead_id):
    """Retrieve all salespersons for a given lead ID."""
    with DBSession() as session:
        results = session.query(Salesperson, Lead_Salesperson.is_primary)\
            .join(
                Lead_Salesperson,
                Salesperson.id == Lead_Salesperson.salesperson_id,
            ).filter(Lead_Salesperson.lead_id == lead_id)\
            .all()
        return results

def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve a list of all salespersons for a given lead id."""
    logger.info(f"Event: {event}")

    try:
        lead_id = event["pathParameters"]["lead_id"]
        salespersons = get_salespersons_for_lead(lead_id)

        if not salespersons:
            logger.error(f"No salespersons found for lead {lead_id}")
            return {
                "statusCode": 404,
                "body": dumps({"error": "No salespersons found for the given lead."})
            }

        primary_salesperson = None
        other_salespersons = []

        for salesperson, is_primary in salespersons:
            salesperson_record = {
                "first_name": salesperson.first_name,
                "last_name": salesperson.last_name,
                "email": salesperson.email,
                "phone": salesperson.phone,
                "position_name": salesperson.position_name,
                "is_primary": is_primary
            }

            if is_primary:
                primary_salesperson = salesperson_record
            else:
                other_salespersons.append(salesperson_record)

        # Put the primary salesperson first in the array
        if primary_salesperson:
            salespersons_list = [primary_salesperson] + other_salespersons
        else:
            salespersons_list = other_salespersons

        return {
            "statusCode": 200,
            "body": dumps(salespersons_list)
        }

    except Exception as e:
        logger.exception(f"Error retrieving salespersons: {e}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
