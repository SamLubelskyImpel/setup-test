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



def check_integration_partner_exists(session, impel_integration_partner_id):
    """
    Checks if the integration partner with the given impel_integration_partner_id exists.
    
    Args:
        session: Database session.
        impel_integration_partner_id: id used to refer to the dealer so for example the dealership Kia Mexico we would call kia_mexico.
    
    Returns:
        The IntegrationPartner object if it exists, otherwise raises a ValueError.
    """
    integration_partner = session.query(IntegrationPartner).filter_by(
        impel_integration_partner_id=impel_integration_partner_id
    ).first()

    if not integration_partner:
        raise ValueError(f"Integration partner with ID '{impel_integration_partner_id}' does not exist.")
    
    return integration_partner


def create_new_dealer(session, impel_dealer_id, location_name, state, city, zip_code, full_name):
    """
    Creates a new dealer in the database if the dealer does not already exist.

    Args:
        session: Database session
        impel_dealer_id (str): The unique identifier for the dealer.
        location_name (str): Dealer's location.
        state (str): The state where the dealer is located.
        city (str): The city where the dealer is located.
        zip_code (str): The zip code of the dealer's location.
        full_name (str): The full name of the dealer.

    Returns:
        Dealer: The newly created `Dealer` object.

    Raises:
        IntegrityError: If a dealer with the specified `impel_dealer_id` already exists in the database.
    """
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


def create_dealer_integration(session, integration_partner, impel_dealer_id, dms_id, dealer_id, is_active=True):
    """
    Creates a new dealer integration in the database if it does not already exist.

    Args:
        session (sqlalchemy.orm.session.Session): The active database session for querying and committing changes.
        integration_partner (IntegrationPartner): The `IntegrationPartner` object retrieved from the database using the impel_integration_partner_id.
        impel_dealer_id (str): The unique identifier for the dealer.
        dms_id (str): Id the DMS integration partner uses to refer to the dealer.
        dealer_id (int): The unique database identifier of the dealer.
        is_active (bool, optional): A flag indicating if the dealer integration is active. Defaults to True.

    Returns:
        DealerIntegrationPartner: The newly created `DealerIntegrationPartner` object.

    Raises:
        ValueError: If an integration with the given `dealer_id`, `integration_partner_id`, and `dms_id` 
        already exists in the database.
    """

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
    """
    Handles the creation of a new dealer and its integration.

    This function extracts dealer and integration details from the event, checks if the 
    integration partner exists. It returns an HTTP response with the result.

    Args:
        event (dict): Contains the request body with dealer and integration details

    Returns:
        dict: A response with an HTTP status code (200, 400, 409, 500) and a JSON-encoded message.
    """
    logger.info(f"Received request to create a new dealer: {event}")
    body = json.loads(event['body'])
    logger.info(body)

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
            integration_partner = check_integration_partner_exists(session, impel_integration_partner_id)
            dealer = create_new_dealer(session, impel_dealer_id, location_name, state, city, zip_code, full_name)
            dealer_id = dealer.id
            dealer_integration = create_dealer_integration(session, integration_partner, impel_dealer_id, dms_id, dealer_id)
            logger.info(f"Dealer integration created: {dealer_integration}")
        except IntegrityError:
            error_message = f"Dealer with impel_dealer_id '{impel_dealer_id}' already exists."
            logger.error(error_message)
            return {"statusCode": 409, "body": dumps({"message": error_message})}
        except ValueError as e:
            logger.error(e)
            return {"statusCode": 400, "body": dumps({"message": str(e)})}
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {"statusCode": 500, "body": dumps({"message": str(e)})}

    return {"statusCode": 200, "body": dumps({"message": "Dealer created successfully."})}


def lambda_handler(event, context):
    """Run create dealer API."""

    logger.info(f"Event: {event}")
    try:
        return create_dealer_handler(event, context)
    except Exception:
        logger.exception("Error creating dealer.")
        raise
