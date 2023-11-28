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
        results = session.query(Salesperson)\
            .join(
                Lead_Salesperson,
                Salesperson.id == Lead_Salesperson.salesperson_id,
            ).filter(Lead_Salesperson.lead_id == lead_id)\
            .all()
        return results


def update_salespersons(lead_id, dealer_id, updated_salespersons):
    """Update salespersons for a given lead ID."""
    to_delete = []

    # Update existing salespersons, filter out old salespersons
    for salesperson in get_salespersons_for_lead(lead_id):
        for new_salesperson in updated_salespersons:
            if new_salesperson["crm_salesperson_id"] == salesperson.crm_salesperson_id:
                new_salesperson.update({"salesperson_id": salesperson.id})
                break
        else:
            to_delete.append(salesperson.id)

    with DBSession() as session:
        # Delete lead salespersons/salespersons that are not in the new list
        session.query(
                Lead_Salesperson
            ).filter(
                Lead_Salesperson.lead_id == lead_id,
                Lead_Salesperson.salesperson_id.in_([s for s in to_delete])
            ).delete(synchronize_session=False)
        session.commit()

        for person in updated_salespersons:
            salesperson_id = person.get("salesperson_id", "")

            if salesperson_id:
                # Update existing salespersons
                session.query(
                        Lead_Salesperson,
                    ).filter(
                        Lead_Salesperson.lead_id == lead_id,
                        Lead_Salesperson.salesperson_id == salesperson_id
                    ).update(
                        {
                            "is_primary": person.get("is_primary", False)
                        }
                    )

                session.query(
                        Salesperson,
                    ).filter(
                        Salesperson.id == salesperson_id
                    ).update(
                        {
                            "crm_salesperson_id": person["crm_salesperson_id"],
                            "first_name": person["first_name"],
                            "last_name": person["last_name"],
                            "phone": person["phone"],
                            "email": person["email"],
                            "position_name": person.get("position_name", "")
                        }
                    )
                session.commit()
            else:
                # Check if salesperson already exists for that dealer
                salesperson = session.query(
                        Salesperson
                    ).filter(
                        Salesperson.dealer_id == dealer_id,
                        Salesperson.crm_salesperson_id == person["crm_salesperson_id"]
                    ).first()
                if salesperson:
                    salesperson.first_name = person["first_name"],
                    salesperson.last_name = person["last_name"],
                    salesperson.phone = person["phone"],
                    salesperson.email = person["email"],
                    salesperson.position_name = person.get("position_name", "")
                    session.commit()
                else:
                    # Create new salesperson
                    salesperson = Salesperson(
                        crm_salesperson_id=person["crm_salesperson_id"],
                        first_name=person["first_name"],
                        last_name=person["last_name"],
                        phone=person["phone"],
                        email=person["email"],
                        position_name=person.get("position_name", ""),
                        dealer_id=dealer_id
                    )
                    session.add(salesperson)
                    session.commit()

                lead_salesperson = Lead_Salesperson(
                    lead_id=lead_id,
                    salesperson_id=salesperson.id,
                    is_primary=person.get("is_primary", False)
                )
                session.add(lead_salesperson)
                person.update({"salesperson_id": salesperson.id})
                session.commit()

    logger.info(f"Deleted Salespersons for lead_id {lead_id}, {to_delete}")
    logger.info(f"Created/Updated Salespersons for lead_id {lead_id}, {updated_salespersons}")
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
                    vin=vehicle.get("vin", None),
                    type=vehicle.get("type", None),
                    vehicle_class=vehicle.get("class", None),
                    mileage=vehicle.get("mileage", None),
                    make=vehicle.get("make", None),
                    model=vehicle.get("model", None),
                    manufactured_year=vehicle.get("year", None),
                    body_style=vehicle.get("body_style", None),
                    transmission=vehicle.get("transmission", None),
                    interior_color=vehicle.get("interior_color", None),
                    exterior_color=vehicle.get("exterior_color", None),
                    trim=vehicle.get("trim", None),
                    price=vehicle.get("price", None),
                    status=vehicle.get("status", None),
                    condition=vehicle.get("condition", None),
                    odometer_units=vehicle.get("odometer_units", None),
                    vehicle_comments=vehicle.get("vehicle_comments", None)
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
