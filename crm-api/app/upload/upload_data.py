"""Upload data to the shared CRM layer."""
import logging
from os import environ
from datetime import datetime
from json import dumps, loads
from typing import Any, List
from sqlalchemy import or_

from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer import Dealer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.lead_salesperson import Lead_Salesperson

from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

consumer_attrs = ['dealer_integration_partner_id', 'crm_consumer_id', 'first_name', 'last_name', 'middle_name',
                  'email', 'phone', 'postal_code', 'address', 'country',
                  'city', 'email_optin_flag', 'sms_optin_flag',
                  'request_product']
salesperson_attrs = ['dealer_integration_partner_id', 'crm_salesperson_id', 'first_name', 'last_name', 'email',
                     'phone', 'position_name', 'is_primary']


def update_attrs(object_name: str, db_object: Any, data: Any, dealer_partner_id: str,
                 allowed_attrs: List[str], request_product) -> None:
    """Update attributes of a database object."""
    additional_attrs = {}
    if object_name == "consumer":
        additional_attrs = {
            "email_optin_flag": data.get("email_optin_flag", True),
            "sms_optin_flag": data.get("sms_optin_flag", True),
            "request_product": request_product
        }

    combined_data = {"dealer_integration_partner_id": dealer_partner_id, **data, **additional_attrs}

    for attr in allowed_attrs:
        if attr in combined_data:
            setattr(db_object, attr, combined_data[attr])


def format_ts(input_ts: str) -> str:
    """Format a timestamp string into a specific format."""
    dt = datetime.strptime(input_ts, "%Y-%m-%dT%H:%M:%SZ")

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
            dealer_partner = session.query(
                DealerIntegrationPartner
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).filter(
                Dealer.product_dealer_id == product_dealer_id,
                or_(DealerIntegrationPartner.is_active.is_(True),
                    DealerIntegrationPartner.is_active_salesai.is_(True),
                    DealerIntegrationPartner.is_active_chatai.is_(True))
            ).first()
            if not dealer_partner:
                logger.error(f"No active dealer found with id {product_dealer_id}.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {product_dealer_id}."})
                }

            dealer_partner_id = dealer_partner.id
            crm_consumer_id = consumer.get("crm_consumer_id")

            # Query for existing consumer
            consumer_db = None
            if crm_consumer_id:
                consumer_db = session.query(Consumer).filter(
                    Consumer.crm_consumer_id == crm_consumer_id,
                    Consumer.dealer_integration_partner_id == dealer_partner_id
                ).first()

            # Create a new consumer if not found
            if not consumer_db:
                consumer_db = Consumer()
            else:
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
                        "statusCode": 409,
                        "body": dumps(msg)
                    }

            # Update consumer attributes
            update_attrs(
                'consumer',
                consumer_db,
                consumer,
                dealer_partner_id,
                consumer_attrs,
                request_product
            )

            lead_obj = Lead(
                crm_lead_id=lead.get("crm_lead_id"),
                status=lead.get("lead_status"),
                substatus=lead.get("lead_substatus"),
                comment=lead.get("lead_comment"),
                origin_channel=lead.get("lead_origin"),
                source_channel=lead.get("lead_source"),
                request_product=request_product,
                lead_ts=format_ts(lead.get("lead_ts"))
            )
            session.add(lead_obj)
            lead_obj.consumer = consumer_db

            for vehicle in vehicles_of_interest:
                vehicle_db = Vehicle(
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
                lead_obj.vehicles.append(vehicle_db)

            crm_salesperson_id = salesperson.get("crm_salesperson_id")
            salesperson_db = None
            if crm_salesperson_id:
                salesperson_db = session.query(Salesperson).filter(
                    Salesperson.crm_salesperson_id == crm_salesperson_id,
                    Salesperson.dealer_integration_partner_id == dealer_partner_id
                ).first()

            if not salesperson_db:
                salesperson_db = Salesperson()

            update_attrs(
                'salesperson',
                salesperson_db,
                salesperson,
                dealer_partner_id,
                salesperson_attrs,
                request_product)

            if not salesperson_db.id:
                session.add(salesperson_db)

            lead_salesperson = Lead_Salesperson(
                is_primary=salesperson.get("is_primary", False)
            )
            lead_salesperson.salesperson = salesperson_db
            lead_obj.lead_salespersons.append(lead_salesperson)

            session.commit()
            lead_id = lead_obj.id

        logger.info(f"Created lead {lead_id}")

        return {
            "statusCode": 200,
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
