"""Upload data to the shared CRM layer."""
import logging
from os import environ
from datetime import datetime
from json import dumps, loads
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer
from crm_orm.models.salesperson import Salesperson

from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

# consumer_attrs = ['crm_consumer_id', 'first_name', 'last_name', 'middle_name', 'email', 'phone', 'postal_code', 'address', 'country', 'city', 'email_optin_flag', 'sms_optin_flag', 'request_product']
# salesperson_attrs = ['crm_salesperson_id', 'first_name', 'last_name', 'email', 'phone', 'position_name']

def update_consumer_attrs(consumer_db, consumer_data, dealer_id, request_product):
    consumer_attrs = {
        "dealer_id": dealer_id,
        "crm_consumer_id": consumer_data.get("crm_consumer_id"),
        "first_name": consumer_data.get("first_name"),
        "last_name": consumer_data.get("last_name"),
        "middle_name": consumer_data.get("middle_name"),
        "email": consumer_data.get("email"),
        "phone": consumer_data.get("phone"),
        "postal_code": consumer_data.get("postal_code"),
        "address": consumer_data.get("address"),
        "country": consumer_data.get("country"),
        "city": consumer_data.get("city"),
        "email_optin_flag": consumer_data.get("email_optin_flag", True),
        "sms_optin_flag": consumer_data.get("sms_optin_flag", True),
        "request_product": request_product
    }

    for attr, value in consumer_attrs.items():
        setattr(consumer_db, attr, value)


def update_salesperson_attrs(salesperson_db, salesperson_data, dealer_id):
    salesperson_attrs = {
        "dealer_id": dealer_id,
        "crm_salesperson_id": salesperson_data.get("crm_salesperson_id"),
        "first_name": salesperson_data.get("first_name"),
        "last_name": salesperson_data.get("last_name"),
        "email": salesperson_data.get("email"),
        "phone": salesperson_data.get("phone"),
        "position_name": salesperson_data.get("position_name")
    }

    for attr, value in salesperson_attrs.items():
        setattr(salesperson_db, attr, value)


def format_ts(input_ts):
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

        request_product = 'CRM'  # ???
        body = loads(event["body"])
        
        for item in body:
            dealer_product_id = item["dealer_product_id"]
            consumer = item["consumer"]
            lead = item["lead"]
            vehicles_of_interest = lead["vehicles_of_interest"]
            salesperson = item["salesperson"]

            with DBSession() as session:
                dealer = session.query(Dealer).filter(Dealer.product_dealer_id == dealer_product_id).first()
                if not dealer:
                    logger.error(f"Dealer {dealer_product_id} not found")
                    return {
                        "statusCode": 404,
                        "body": dumps({"error": f"Dealer {dealer_product_id} not found."})
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
                update_consumer_attrs(consumer_db, consumer, dealer_id, request_product)

                # Add and flush the session if the consumer is new
                if not consumer_db.id:
                    session.add(consumer_db)
                    session.flush()

                lead_db = Lead(
                    consumer_id=consumer_db.id,
                    crm_lead_id=lead.get("lead_id"),
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
                        type=vehicle.get("type"),
                        vehicle_class=vehicle.get("vehicle_class"),
                        mileage=vehicle.get("mileage"),
                        make=vehicle.get("make"),
                        model=vehicle.get("model"),
                        manufactured_year=vehicle.get("manufactured_year"),
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

                update_salesperson_attrs(salesperson_db, salesperson, dealer_id)

                # Add and flush the session if the consumer is new
                if not salesperson_db.id:
                    session.add(salesperson_db)
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
            "body": dumps({"error": "An error occurred while processing the request."})
        }
