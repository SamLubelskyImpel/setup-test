import logging
from os import environ
from json import dumps

from crm_orm.models.lead import Lead
from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Retrieve salesperson data by lead id."""
    logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    with DBSession() as session:
        result = session.query(
            Lead, Salesperson
        ).outerjoin(
            Salesperson, Lead.salesperson_id == Salesperson.id
        ).filter(
            Lead.id == lead_id
        ).first()
        if result is not None:
            lead, salesperson = result
        else:
            logger.error(f"Lead not found {lead_id}")
            return {
                "statusCode": "404"
            }

    if not salesperson:
        logger.error(f"Salesperson not found for lead {lead_id}")
        return {
            "statusCode": "404"
        }

    salesperson_record = {
        "first_name": salesperson.first_name,
        "last_name": salesperson.last_name,
        "email": salesperson.email,
        "phone": salesperson.phone
    }

    return {
        "statusCode": "200",
        "body": dumps(salesperson_record)
    }
