import logging
from json import loads
from os import environ
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Update lead."""
    logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    body = loads(event["body"])
    consumer_id = int(body["consumer_id"])
    salesperson_id = int(body["salesperson_id"])

    # fields_to_update = [
    #     "lead_status", "lead_substatus", "lead_comment", "lead_origin", "lead_source"
    # ]

    with DBSession() as session:
        lead = session.query(Lead).filter(Lead.id == lead_id).first()

        if not lead:
            logger.error(f"Lead not found {lead_id}")
            return {
                "statusCode": "404"
            }
        
        consumer = session.query(
            Consumer
        ).filter(
            Consumer.id == consumer_id
        ).first()

        if not consumer:
            logger.error(f"Consumer not found {consumer_id}")
            return {
                "statusCode": "404"
            }

        salesperson = session.query(
            Salesperson
        ).filter(
            Salesperson.id == salesperson_id
        ).first()

        if not salesperson:
            logger.error(f"Salesperson not found {salesperson_id}")
            return {
                "statusCode": "404"
            }
    
        # for field in fields_to_update:
        #     if field in body:
        #         setattr(consumer, field, body[field])

        session.commit()

    # logger.info(f"Lead is updated {lead_id}")

    return {
        "statusCode": "200"
    }
