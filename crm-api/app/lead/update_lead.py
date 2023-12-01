"""Update lead."""
import logging
from json import loads, dumps
from os import environ
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_salespersons_for_lead(lead_id: str) -> Any:
    """Retrieve all salespersons for a given lead ID."""
    with DBSession() as session:
        results = session.query(Lead_Salesperson).filter(
            Lead_Salesperson.lead_id == lead_id
        ).all()
        return results


def update_salespersons(lead_id, dealer_id, new_salespersons):
    """Update salesperson tables for a given lead ID."""
    # Update Salesperson's that are still assigned to the lead, or create if needed.
    for new_person in new_salespersons:
        with DBSession() as session:
            salesperson = session.query(Salesperson).filter(
                    Salesperson.dealer_id == dealer_id,
                    Salesperson.crm_salesperson_id == new_person["crm_salesperson_id"]
                ).first()
            # Update salesperson if it exists, otherwise create it
            if salesperson:
                salesperson.first_name = new_person.get("first_name", "")
                salesperson.last_name = new_person.get("last_name", "")
                salesperson.phone = new_person.get("phone", "")
                salesperson.email = new_person.get("email", "")
                salesperson.position_name = new_person.get("position_name", "")
                logger.info(f"Updated Salesperson for lead_id {lead_id}, {new_person}")
            else:
                salesperson = Salesperson(
                    crm_salesperson_id=new_person["crm_salesperson_id"],
                    first_name=new_person.get("first_name", ""),
                    last_name=new_person.get("last_name", ""),
                    phone=new_person.get("phone", ""),
                    email=new_person.get("email", ""),
                    position_name=new_person.get("position_name", ""),
                    dealer_id=dealer_id
                )
                session.add(salesperson)
                logger.info(f"Created Salesperson for lead_id {lead_id}, {new_person}")
            session.commit()
            new_person.update({"salesperson_id": salesperson.id})

    # Update Lead_Salesperson records that are still assigned to the lead, or create if needed.
    for new_person in new_salespersons:
        with DBSession() as session:
            lead_salesperson = session.query(Lead_Salesperson).filter(
                Lead_Salesperson.lead_id == lead_id,
                Lead_Salesperson.salesperson_id == new_person["salesperson_id"]
            ).first()
            if lead_salesperson:
                lead_salesperson.is_primary = new_person.get("is_primary", False)
                logger.info(f"Updated Lead_Salesperson for lead_id {lead_id}, {new_person}")
            else:
                lead_salesperson = Lead_Salesperson(
                    lead_id=lead_id,
                    salesperson_id=new_person["salesperson_id"],
                    is_primary=new_person.get("is_primary", False)
                )
                session.add(lead_salesperson)
                logger.info(f"Created Lead_Salesperson for lead_id {lead_id}, {new_person}")
            session.commit()

    removed_salespeople = []
    existing_salespersons = get_salespersons_for_lead(lead_id)
    for existing_person in existing_salespersons:
        if existing_person.salesperson_id not in [new_person["salesperson_id"] for new_person in new_salespersons]:
            removed_salespeople.append(existing_person.salesperson_id)

    # Delete Lead_Salesperson records that are no longer assigned to the lead.
    with DBSession() as session:
        session.query(Lead_Salesperson).filter(
                Lead_Salesperson.lead_id == lead_id,
                Lead_Salesperson.salesperson_id.in_(removed_salespeople)
            ).delete(synchronize_session=False)
        session.commit()
        logger.info(f"Deleted Lead_Salesperson for lead_id {lead_id}, {removed_salespeople}")

    logger.info(f"Salespersons are updated {lead_id}")


def lambda_handler(event: Any, context: Any) -> Any:
    """Update lead."""
    logger.info(f"Event: {event}")

    lead_id = event["pathParameters"]["lead_id"]

    try:
        body = loads(event["body"])
        consumer_id = body.get("consumer_id")
        vehicles_of_interest = body.get('vehicles_of_interest', [])

        lead_field_mapping = {
            "lead_status": "status",
            "lead_substatus": "substatus",
            "lead_comment": "comment",
            "lead_origin": "origin_channel",
            "lead_source": "source_channel"
        }

        with DBSession() as session:
            lead = session.query(Lead).filter(Lead.id == lead_id).first()

            if not lead:
                logger.error(f"Lead not found {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead not found {lead_id}"})
                }

            # Update consumer if provided
            if consumer_id:
                consumer = session.query(
                    Consumer
                ).filter(
                    Consumer.id == consumer_id
                ).first()

                if not consumer:
                    logger.error(f"Consumer not found {consumer_id}")
                    return {
                        "statusCode": 404,
                        "body": dumps({"error": f"Consumer not found {consumer_id}"})
                    }

                lead.consumer_id = consumer_id

            # Get dealer_id
            dealer_id = lead.consumer.dealer_id

            # Update lead fields if provided
            for received_field, db_field in lead_field_mapping.items():
                if received_field in body:
                    new_value = body[received_field]
                    setattr(lead, db_field, new_value)

            # Add new vehicles if provided
            for vehicle in vehicles_of_interest:
                vehicle = Vehicle(
                    lead_id=lead.id,
                    vin=vehicle.get("vin"),
                    type=vehicle.get("type"),
                    vehicle_class=vehicle.get("class"),
                    mileage=vehicle.get("mileage"),
                    make=vehicle.get("make"),
                    model=vehicle.get("model"),
                    manufactured_year=vehicle.get("year"),
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
                lead.vehicles.append(vehicle)

            session.commit()
            logger.info(f"Lead is updated {lead_id}")

        update_salespersons(lead_id, dealer_id, body.get("salespersons", []))

        return {
            "statusCode": 200,
            "body": dumps({"message": "Lead updated successfully"})
        }
    except Exception as e:
        logger.exception(f"Error updating lead: {e}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
