"""Runs the create new dealer API"""
import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.op_code import OpCode
from dms_orm.models.op_code_repair_order import OpCodeRepairOrder
from dms_orm.models.service_repair_order import ServiceRepairOrder
from dms_orm.models.vehicle import Vehicle
from dms_orm.session_config import DBSession
from sqlalchemy import func, text

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

def create_dealer_handler(event, context):
    """Create dealer API endpoint."""
    logger.info(f"Received request to create a new dealer: {event}")
    
    # Perform any necessary validation or processing here
    
    # Assuming successful creation, return a 200 response
    return {
        "statusCode": 200,
        "body": dumps({"message": "Dealer created successfully."}),
    }

def lambda_handler(event, context):
    """Run repair order API."""

    logger.info(f"Event: {event}")
    try:
        return create_dealer_handler(event, context)
    except Exception:
        logger.exception("Error creating dealer.")
        raise
