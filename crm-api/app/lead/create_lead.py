"""Create lead in the shared CRM layer."""
import logging
import pytz
from dateutil import parser
import boto3
import json
import requests
from os import environ
from datetime import datetime
from json import dumps, loads
from typing import Any, List

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.session_config import DBSession
from utils import send_email_notification

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
DA_SECRET_KEY = environ.get("DA_SECRET_KEY")

sm_client = boto3.client("secretsmanager")

salesperson_attrs = ['dealer_integration_partner_id', 'crm_salesperson_id', 'first_name', 'last_name', 'email',
                     'phone', 'position_name', 'is_primary']

class EventListenerError(Exception):
    pass


def update_attrs(db_object: Any, data: Any, dealer_partner_id: str,
                 allowed_attrs: List[str], request_product) -> None:
    """Update attributes of a database object."""
    combined_data = {"dealer_integration_partner_id": dealer_partner_id, **data}

    for attr in allowed_attrs:
        if attr in combined_data:
            setattr(db_object, attr, combined_data[attr])


def get_dealer_timezone(dealer_integration_partner_id: str) -> Any:
    """Get the timezone of the dealer."""
    with DBSession() as session:
        dealer_timezone = session.query(Dealer.metadata_['timezone']).filter(
            Dealer.id == dealer_integration_partner_id
        ).first()

        timezone = dealer_timezone[0] if dealer_timezone else "UTC"
        logger.info(f"Dealer {dealer_integration_partner_id} timezone: {timezone}")
        return timezone


def process_lead_ts(input_ts: Any, dealer_timezone: Any) -> Any:
    """
    Process an input timestamp based on whether it's in UTC, has an offset, or is local without an offset.

    Apply dealer_timezone if it is present.
    """
    try:
        parsed_ts = parser.parse(input_ts)

        # Check if the timestamp is already in UTC (ends with 'Z')
        if parsed_ts.tzinfo is not None and parsed_ts.tzinfo.utcoffset(parsed_ts) is not None:
            # Timestamp is either UTC or has an offset; return in ISO format
            return parsed_ts.isoformat()

        # If the timestamp is local (no 'Z' or offset) and dealer_timezone is provided
        if dealer_timezone:
            dealer_tz = pytz.timezone(dealer_timezone)
            # Localize the timestamp to the dealer's timezone
            localized_ts = dealer_tz.localize(parsed_ts)
            return localized_ts.isoformat()
        else:
            # Timestamp is local without a specified dealer_timezone
            # Returning the naive timestamp as it is
            return parsed_ts.isoformat()

    except Exception as e:
        logger.info(f"Error processing timestamp: {input_ts}, Dealer timezone: {dealer_timezone}. Error: {e}")
        return None


def get_secret(secret_name: Any, secret_key: Any) -> Any:
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data


def send_to_event_listener(lead_id: int) -> None:
    """Send notification to DA Event listener."""
    try:
        listener_secrets = get_secret(
            secret_name="crm-integrations-partner", secret_key=DA_SECRET_KEY
        )

        data = {
            "message": "New Lead available from CRM API",
            "lead_id": lead_id,
        }
        response = requests.post(
            url=listener_secrets["API_URL"],
            headers={"Authorization": listener_secrets["API_TOKEN"]},
            json=data,
            timeout=30,
        )
        logger.info(f"DA Event Listener responded with status: {response.status_code}")
        response.raise_for_status()

    except requests.exceptions.Timeout:
        logger.error(
            f"Timeout occurred calling DA Event Listener for the lead {lead_id}"
        )
    except Exception as e:
        message = f"Error sending the lead {lead_id} to DA Event Listener: {e}"
        logger.error(message)
        send_email_notification(message)
        raise EventListenerError


def lambda_handler(event: Any, context: Any) -> Any:
    """Create lead."""
    try:
        logger.info(f"Event: {event}")

        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        consumer_id = body["consumer_id"]
        salespersons = body.get("salespersons", [])
        crm_lead_id = body.get("crm_lead_id")
        lead_ts=body.get("lead_ts", datetime.utcnow())

        with DBSession() as session:
            consumer = session.query(
                Consumer
            ).filter(
                Consumer.id == consumer_id
            ).first()

            if not consumer:
                logger.error(f"Consumer {consumer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Consumer {consumer_id} not found. Lead failed to be created."})
                }

            dealer_integration_partner_id = consumer.dealer_integration_partner_id
            dealer_timezone = get_dealer_timezone(dealer_integration_partner_id)

            logger.info(f"Original timestamp: {lead_ts}")
            lead_ts = process_lead_ts(lead_ts, dealer_timezone)
            logger.info(f"Processed timestamp: {lead_ts}")

            integration_partner = consumer.dealer_integration_partner.integration_partner

            # Query for existing lead
            if crm_lead_id:
                lead_db = session.query(
                    Lead
                ).filter(
                    Lead.consumer_id == consumer_id,
                    Lead.crm_lead_id == crm_lead_id
                ).first()

                if lead_db:
                    logger.error(f"Lead {crm_lead_id} already exists")
                    return {
                        "statusCode": 409,
                        "body": dumps({"error": f"Lead with CRM ID {crm_lead_id} already exists for consumer {consumer_id}. lead_id: {lead_db.id}"})
                    }

            # Create lead
            lead = Lead(
                consumer_id=consumer_id,
                status=body["lead_status"],
                substatus=body["lead_substatus"],
                comment=body["lead_comment"],
                origin_channel=body["lead_origin"],
                source_channel=body["lead_source"],
                crm_lead_id=crm_lead_id,
                request_product=request_product,
                lead_ts=lead_ts,
                metadata_=body.get("metadata"),
            )

            session.add(lead)

            # Create vehicles of interest
            vehicles_of_interest = body["vehicles_of_interest"]
            for vehicle in vehicles_of_interest:
                vehicle = Vehicle(
                    lead_id=lead.id,
                    vin=vehicle.get("vin"),
                    stock_num=vehicle.get("stock_number"),
                    type=vehicle.get("type"),
                    vehicle_class=vehicle.get("class"),
                    mileage=vehicle.get("mileage"),
                    make=vehicle.get("make"),
                    model=vehicle.get("model"),
                    manufactured_year=vehicle.get("year"),
                    oem_name=vehicle.get("oem_name"),
                    body_style=vehicle.get("body_style"),
                    transmission=vehicle.get("transmission"),
                    interior_color=vehicle.get("interior_color"),
                    exterior_color=vehicle.get("exterior_color"),
                    trim=vehicle.get("trim"),
                    price=vehicle.get("price"),
                    status=vehicle.get("status"),
                    condition=vehicle.get("condition"),
                    odometer_units=vehicle.get("odometer_units"),
                    vehicle_comments=vehicle.get("vehicle_comments"),
                    crm_vehicle_id=vehicle.get("crm_vehicle_id")
                )
                lead.vehicles.append(vehicle)

            if salespersons:
                dealer_partner_id = consumer.dealer_integration_partner_id
                for salesperson in salespersons:
                    # Create salesperson
                    crm_salesperson_id = salesperson.get("crm_salesperson_id")

                    # Query for existing salesperson
                    salesperson_db = None
                    if crm_salesperson_id:
                        salesperson_db = session.query(Salesperson).filter(
                            Salesperson.crm_salesperson_id == crm_salesperson_id,
                            Salesperson.dealer_integration_partner_id == dealer_partner_id
                        ).first()

                    if not salesperson_db:
                        salesperson_db = Salesperson()

                    update_attrs(salesperson_db, salesperson, dealer_partner_id, salesperson_attrs, request_product)

                    if not salesperson_db.id:
                        session.add(salesperson_db)
                        session.flush()

                    # Create lead salesperson
                    lead_salesperson = Lead_Salesperson(
                        lead_id=lead.id,
                        salesperson_id=salesperson_db.id,
                        is_primary=salesperson.get("is_primary", False)
                    )
                    session.add(lead_salesperson)

            session.commit()
            lead_id = lead.id
            integration_partner_name = integration_partner.impel_integration_partner_name

        logger.info(f"Created lead {lead_id}")
        logger.info(f"Integration partner: {integration_partner_name}")

        if integration_partner_name == 'REYREY' or integration_partner_name == 'DEALERPEAK':
            send_to_event_listener(lead_id)
            logger.info(f"Successfully sent the lead {lead_id} to DA")

        return {
            "statusCode": "201",
            "body": dumps({"lead_id": lead_id})
        }

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
