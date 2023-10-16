"""Retrieve salespersons data from the shared CRM layer."""
import logging
from os import environ
from json import dumps
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve a list of all salespersons for a given lead id."""
    logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    with DBSession() as session:
        results = session.query(
            Salesperson
        ).join(
            Lead, Lead.salesperson_id == Salesperson.id
        ).filter(
            Lead.id == lead_id
        ).all()

        if not results:
            logger.error(f"No salespersons found for lead {lead_id}")
            return {
                "statusCode": "404"
            }

        # Find the primary salesperson (if any)
        primary_salesperson = None
        other_salespersons = []

        for salesperson in results:
            salesperson_record = {
                "first_name": salesperson.first_name,
                "last_name": salesperson.last_name,
                "email": salesperson.email,
                "phone": salesperson.phone,
                "position_name": salesperson.position_name,
                "is_primary": salesperson.is_primary
            }

            if salesperson.is_primary:
                primary_salesperson = salesperson_record
            else:
                other_salespersons.append(salesperson_record)

        # Combine primary salesperson (if found) and other salespersons
        salespersons_list = []
        if primary_salesperson:
            salespersons_list.append(primary_salesperson)
        salespersons_list.extend(other_salespersons)

    return {
        "statusCode": "200",
        "body": dumps(salespersons_list)
    }
