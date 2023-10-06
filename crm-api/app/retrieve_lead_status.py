import logging
from os import environ
from json import dumps

from crm_orm.models.lead import Lead
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Retrieve lead status."""
    logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    with DBSession as session:
        lead = session.query(
            Lead
        ).filter(
            Lead.id == lead_id
        ).first()

    if not lead:
        logger.error(f"Lead not found {lead_id}")
        return {
            "statusCode": "404"
        }

    lead_status_record = {
        "lead_status": lead.status
    }

    return {
        "statusCode": "200",
        "body": dumps(lead_status_record)
    }
