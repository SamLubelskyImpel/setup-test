"""Get lead updates from the shared layer."""
import logging
from json import dumps
from os import environ

from crm_orm.models.lead import Lead
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Get lead updates."""
    logger.info(f"Event: {event}")

    lead_id = event["lead_id"]
    # dealer_partner_id = event["dealer_integration_partner_id"]
    # crm_lead_id = event["crm_lead_id"]
    # crm_dealer_id = event["crm_dealer_id"]

    salespersons = []
    with DBSession() as session:
        lead_db = session.query(Lead).filter(Lead.id == lead_id).first()
        if not lead_db:
            logger.error(f"Lead {lead_id} not found.")
            raise

        status = lead_db.status
        salespersons_db = session.query(Salesperson, Lead_Salesperson.is_primary)\
            .join(
                Lead_Salesperson,
                Salesperson.id == Lead_Salesperson.salesperson_id,
            ).filter(Lead_Salesperson.lead_id == lead_id)\
            .all()

        for salesperson, is_primary in salespersons_db:
            salesperson_record = {
                "crm_salesperson_id": salesperson.crm_salesperson_id,
                "first_name": salesperson.first_name,
                "last_name": salesperson.last_name,
                "email": salesperson.email,
                "phone": salesperson.phone,
                "position_name": salesperson.position_name,
                "is_primary": is_primary
            }
            salespersons.append(salesperson_record)
    return {
        "statusCode": 200,
        "body": dumps({
            "status": status,
            "salespersons": salespersons
        })
    }
