"""Create lead in the shared CRM layer."""
import pytz
import boto3
import logging
from os import environ
from dateutil import parser
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

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")

salesperson_attrs = ['dealer_integration_partner_id', 'crm_salesperson_id', 'first_name', 'last_name', 'email',
                     'phone', 'position_name', 'is_primary']

def get_lambda_arn(partner_name: str) -> Any:
    """Get lambda ARN from S3."""
    s3_key = f"configurations/{ENVIRONMENT}_GENERAL.json"
    try:
        s3_object = loads(
                s3_client.get_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )
        lambda_arn = s3_object.get("adf_assembler_arn")
    except Exception as e:
        logger.error(f"Failed to retrieve lambda ARN from S3 config. {e}")
    return lambda_arn

def invoke_lambda(body: dict, lambda_arn: str) -> Any:
    """Create ADF data."""
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="Event",
        Payload=dumps(body),
    )
    logger.info(f"Response from lambda: {response}")
    return

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
                source_detail=body.get("lead_source_detail"),
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
                    crm_vehicle_id=vehicle.get("crm_vehicle_id"),
                    trade_in_vin=vehicle.get("trade_in_vin"),
                    trade_in_year=vehicle.get("trade_in_year"),
                    trade_in_make=vehicle.get("trade_in_make"),
                    trade_in_model=vehicle.get("trade_in_model")
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

        logger.info(f"Created lead {lead_id}")

        if request_product == 'chat_ai':
            lambda_arn = get_lambda_arn(request_product)
            if lambda_arn:
                logger.info(f"Lambda ARN detected for partner {request_product}. Creating adf on lead_id: {lead_id}.")
                try:
                    invoke_lambda({"lead_id":lead_id}, lambda_arn)
                except Exception as e:
                    logger.error(f"Failed to create adf. {e}")
            else:
                logger.warning(f"[crm-api.create_lead] Something is wrong with lambda_arn: {lambda_arn}")
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
