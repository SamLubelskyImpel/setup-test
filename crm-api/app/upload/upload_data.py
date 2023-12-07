"""Upload data to the shared CRM layer."""
import logging
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

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

consumer_attrs = ['crm_consumer_id', 'first_name', 'last_name', 'middle_name',
                  'email', 'phone', 'postal_code', 'address', 'country',
                  'city', 'email_optin_flag', 'sms_optin_flag',
                  'request_product']
salesperson_attrs = ['crm_salesperson_id', 'first_name', 'last_name', 'email',
                     'phone', 'position_name']


def update_attrs(db_object: Any, data: Any, dealer_id: str,
                 allowed_attrs: List[str],
                 additional_attrs: Any = None) -> None:
    """Update attributes of a database object."""
    if additional_attrs is None:
        additional_attrs = {}

    combined_data = {"dealer_id": dealer_id, **data, **additional_attrs}

    for attr in allowed_attrs:
        if attr in combined_data:
            setattr(db_object, attr, combined_data[attr])


def format_ts(input_ts: str) -> str:
    """Format a timestamp string into a specific format."""
    if 'T' in input_ts:
        input_format = "%Y-%m-%dT%H:%M:%S"
    else:
        input_format = "%B, %d %Y %H:%M:%S"

    dt = datetime.strptime(input_ts, input_format)

    output_format = "%Y-%m-%d %H:%M:%S.000"
    return dt.strftime(output_format)


def lambda_handler(event: Any, context: Any) -> Any:
    """Upload unified data to the CRM API database."""
    try:
        logger.info(f"Event: {event}")

        request_product = event["headers"]["partner_id"]
        body = loads(event["body"])
        logger.info(f"Body: {body}")

        product_dealer_id = body["product_dealer_id"]
        consumer = body["consumer"]
        lead = body["lead"]
        vehicles_of_interest = lead["vehicles_of_interest"]
        salesperson = body["salesperson"]

        with DBSession() as session:
            dealer = session.query(Dealer).filter(Dealer.product_dealer_id == product_dealer_id).first()
            if not dealer:
                logger.error(f"Dealer {product_dealer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Dealer {product_dealer_id} not found."})
                }

            dealer_id = dealer.id

            crm_consumer_id = consumer.get("crm_consumer_id")

            # Query for existing consumer
            consumer_db = session.query(Consumer).filter(
                Consumer.crm_consumer_id == crm_consumer_id,
                Consumer.dealer_id == dealer.id
            ).first()

            # Create a new consumer if not found
            if not consumer_db:
                consumer_db = Consumer()

            # Update consumer attributes
            update_attrs(
                consumer_db,
                consumer,
                dealer_id,
                consumer_attrs
            )

            # Add and flush the session if the consumer is new
            if not consumer_db.id:
                session.add(consumer_db)
                session.flush()

            existing_lead = session.query(
                Lead
            ).filter(
                Lead.crm_lead_id == lead.get("crm_lead_id"),
                Lead.consumer_id == consumer_db.id
            ).first()

            if existing_lead:
                msg = f"Lead with this crm_lead_id and customer_id already exists. Lead id {existing_lead.id}"
                logger.info(msg)
                return {
                    "statusCode": "409",
                    "body": dumps(msg)
                }

            lead_db = Lead(
                consumer_id=consumer_db.id,
                crm_lead_id=lead.get("crm_lead_id"),
                status=lead.get("status"),
                substatus=lead.get("substatus"),
                comment=lead.get("comment"),
                origin_channel=lead.get("origin_channel"),
                source_channel=lead.get("source_channel"),
                request_product=request_product,
                lead_ts=format_ts(lead.get("lead_ts"))
            )

            session.add(lead_db)

            for vehicle in vehicles_of_interest:
                vehicle_db = Vehicle(
                    lead_id=lead_db.id,
                    crm_vehicle_id=vehicle.get("crm_vehicle_id"),
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
                    vehicle_comments=vehicle.get("vehicle_comments")
                )
                lead_db.vehicles.append(vehicle_db)

            crm_salesperson_id = salesperson.get("crm_salesperson_id")
            salesperson_db = session.query(Salesperson).filter(
                Salesperson.crm_salesperson_id == crm_salesperson_id,
                Salesperson.dealer_id == dealer_id
            ).first()

            if not salesperson_db:
                salesperson_db = Salesperson()

            update_attrs(
                salesperson_db,
                salesperson,
                dealer_id,
                salesperson_attrs)

            if not salesperson_db.id:
                session.add(salesperson_db)
                session.flush()

            lead_salesperson = Lead_Salesperson(
                lead_id=lead_db.id,
                salesperson_id=salesperson_db.id,
                is_primary=salesperson.get("is_primary", False)
            )
            session.add(lead_salesperson)

            session.flush()
            session.commit()
            lead_id = lead_db.id

        logger.info(f"Created lead {lead_id}")

        return {
            "statusCode": "201",
            "body": dumps({"lead_id": lead_id})
        }

    except Exception as e:
        logger.exception(f"Error uploading data to db: {e}.")
        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "error": "An error occurred while processing the request."
                }
            )
        }
