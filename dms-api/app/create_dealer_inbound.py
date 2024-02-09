"""Runs the create new dealer API"""
import logging
import json
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
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def create_new_dealer(session, impel_dealer_id, location_name, state, city, zip_code, full_name):
    existing_dealer = session.query(Dealer).filter_by(impel_dealer_id=impel_dealer_id).first()
    
    if existing_dealer:
        raise IntegrityError(f"Dealer with impel_dealer_id '{impel_dealer_id}' already exists.", '', '')
    new_dealer = Dealer(
        impel_dealer_id=impel_dealer_id,
        db_creation_date=datetime.utcnow(),
        location_name = location_name,
        state = state,
        city = city,
        zip_code = zip_code,
        full_name = full_name
    )
    session.add(new_dealer)
    session.commit()
    return new_dealer  


def create_dealer_integration(session, impel_integration_partner_id, impel_dealer_id, dms_id, dealer_id, is_active=True):
    integration_partner = session.query(IntegrationPartner).filter_by(
        impel_integration_partner_id=impel_integration_partner_id
        ).first()
    
    if not integration_partner:
        raise ValueError(f"Integration partner {impel_integration_partner_id} does not exist.")

    existing_integration = session.query(DealerIntegrationPartner).filter_by(
        dealer_id=dealer_id , 
        integration_partner_id=integration_partner.id, 
        dms_id=dms_id
    ).first()
    
    if existing_integration:
        raise ValueError(f"Dealer Integration {existing_integration.id} already exists.")
    
    new_integration = DealerIntegrationPartner(
        integration_partner_id=integration_partner.id, 
        dealer_id=dealer_id, 
        dms_id=dms_id, 
        is_active=is_active,
        db_creation_date=datetime.utcnow(),
    )
    session.add(new_integration)
    session.commit()
    return new_integration


def create_dealer_handler(event, context):
    logger.info(f"Received request to create a new dealer: {event}")
    body = json.loads(event['body'])

    impel_integration_partner_id = body.get('impel_integration_partner_id')
    impel_dealer_id = body.get('impel_dealer_id')
    dms_id = body.get('dms_id')
    location_name = body.get('location_name')
    state = body.get('state')
    city = body.get('city')
    zip_code = body.get('zip_code')
    full_name = body.get('full_name')

    with DBSession() as session:
        try:
            dealer = create_new_dealer(session, impel_dealer_id, location_name, state, city, zip_code, full_name)
            dealer_id = dealer.id
        except IntegrityError:
            raise ValueError(f"Dealer with impel_dealer_id '{impel_dealer_id}' already exists.")
        try:
            dealer_integration = create_dealer_integration(session, impel_integration_partner_id, impel_dealer_id, dms_id, dealer_id)
            logger.info(f"Dealer integration created: {dealer_integration}")
        except ValueError as e:
            logger.error(e)
            return {"statusCode": 400, "body": dumps({"message": str(e)})}
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {"statusCode": 500, "body": dumps({"message": "Internal server error"})}

    return {"statusCode": 200, "body": dumps({"message": "Dealer created successfully."})}


def lambda_handler(event, context):
    """Run repair order API."""

    logger.info(f"Event: {event}")
    try:
        return create_dealer_handler(event, context)
    except Exception:
        logger.exception("Error creating dealer.")
        raise
