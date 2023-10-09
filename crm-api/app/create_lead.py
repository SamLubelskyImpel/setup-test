import logging
from os import environ
from datetime import datetime
from json import dumps, loads

from crm_orm.models.dealer import Dealer
from crm_orm.models.lead import Lead
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.salesperson import Salesperson
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Create lead."""
    logger.info(f"Event: {event}")

    body = loads(event["body"])
    request_product = event["headers"]["partner_id"]
    dealer_id = event["headers"]["dealer_id"]
    consumer_id = int(body["consumer_id"])
    salesperson_id = int(body["salesperson_id"])

    with DBSession() as session:
        dealer = session.query(
            Dealer
        ).filter(
            Dealer.product_dealer_id == dealer_id
        ).first()
        if not dealer:
            logger.error(f"Dealer not found {dealer_id}")
            return {
                "statusCode": "404",
                "message": f"Dealer not found {dealer_id}"
            }

        consumer = session.query(
            Consumer
        ).filter(
            Consumer.id == consumer_id
        ).first()
        if not consumer:
            logger.error(f"Consumer not found {consumer_id}")
            return {
                "statusCode": "404",
                "message": f"Consumer not found {consumer_id}"
            }

        salesperson = session.query(
            Salesperson
        ).filter(
            Salesperson.id == salesperson_id
        ).first()
        if not salesperson:
            logger.error(f"Salesperson not found {salesperson_id}")
            return {
                "statusCode": "404",
                "message": f"Salesperson not found {salesperson_id}"
            }

        # Create lead
        lead = Lead(
            consumer_id=consumer_id,
            salesperson_id=body.get("salesperson_id", ""),
            status=body["lead_status"],
            substatus=body["lead_substatus"],
            comment=body["lead_comment"],
            origin_channel=body["lead_origin"],
            source_channel=body["lead_source"],
            request_product=request_product,
            lead_ts=datetime.utcnow(),
            db_creation_date=datetime.utcnow(),
            db_update_date=datetime.utcnow(),
            db_update_role="system"
        )

        session.add(lead)

        # Create vehicles of interest
        vehicles_of_interest = body["vehicles_of_interest"]
        for vehicle in vehicles_of_interest:
            vehicle = Vehicle(
                lead_id=lead.id,
                vin=vehicle.get("vin", ""),
                type=vehicle.get("type", ""),
                vehicle_class=vehicle.get("vehicle_class", ""),
                mileage=vehicle.get("mileage", ""),
                make=vehicle.get("make", ""),
                model=vehicle.get("model", ""),
                manufactured_year=vehicle.get("manufactured_year", ""),
                body_style=vehicle.get("body_style", ""),
                transmission=vehicle.get("transmission", ""),
                interior_color=vehicle.get("interior_color", ""),
                exterior_color=vehicle.get("exterior_color", ""),
                trim=vehicle.get("trim", ""),
                price=vehicle.get("price", ""),
                status=vehicle.get("status", ""),
                condition=vehicle.get("condition", ""),
                odometer_units=vehicle.get("odometer_units", ""),
                vehicle_comments=vehicle.get("vehicle_comments", ""),
                db_creation_date=datetime.utcnow(),
                db_update_date=datetime.utcnow(),
                db_update_role="system",
            )
            session.add(vehicle)
        
        session.commit()

    logger.info(f"Created lead {lead.id}")

    return {
        "statusCode": "200",
        "body": dumps({"leadId": lead.id})
    }
