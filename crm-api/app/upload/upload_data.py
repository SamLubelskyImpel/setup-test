"""Create lead in the shared CRM layer."""
import logging
from os import environ
from datetime import datetime
from json import dumps, loads
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.salesperson import Salesperson

from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Upload unified data to the CRM API database."""
    try:
        logger.info(f"Event: {event}")

        body = loads(event["body"])

        return {
            "statusCode": "200"
        }

    except Exception as e:
        logger.exception(f"Error uploading data to db: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
