import logging
from os import environ
from json import dumps, loads
from datetime import datetime

from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Update salesperson by salesperson id."""
    logger.info(f"Event: {event}")

    body = loads(event["body"])
    salesperson_id = event["pathParameters"]["salesperson_id"]

    first_name = body["first_name"]
    last_name = body["last_name"]
    email = body["email"]
    phone = body.get("phone")

    with DBSession() as session:
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

        salesperson.first_name = first_name
        salesperson.last_name = last_name
        salesperson.email = email
        salesperson.phone = phone
        salesperson.db_update_date = datetime.utcnow()
        salesperson.update_role = 'system'

        session.commit()
        salesperson_id = salesperson.id

    logger.info(f"Salesperson updated {salesperson_id}")

    return {
        "statusCode": "200",
        "body": dumps({"SalespersonId": salesperson_id})
    }
