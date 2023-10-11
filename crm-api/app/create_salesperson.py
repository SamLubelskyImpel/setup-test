import logging
from os import environ
from datetime import datetime
from json import dumps, loads

from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Create salesperson."""
    logger.info(f"Event: {event}")

    body = loads(event["body"])

    with DBSession() as session:
        # Create salesperson
        salesperson = Salesperson(
            first_name=body["first_name"],
            last_name=body["last_name"],
            email=body["email"],
            phone=body["phone"],
            db_creation_date=datetime.utcnow(),
            db_update_date=datetime.utcnow(),
            db_update_role="system"
        )

        session.add(salesperson)
        session.commit()

        salesperson_id = salesperson.id

    logger.info(f"Created salesperson {salesperson_id }")

    return {
        "statusCode": "200",
        "body": dumps({"salespersonId": salesperson_id})
    }