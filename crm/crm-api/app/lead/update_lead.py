"""Update lead."""
import logging
from json import loads, dumps
from os import environ
from typing import Any
from utils import send_alert_notification

from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.models.vehicle import Vehicle
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_salespersons_for_lead(session, lead_id: str) -> Any:
    """Retrieve all salespersons for a given lead ID."""
    results = session.query(Lead_Salesperson).filter(
        Lead_Salesperson.lead_id == lead_id
    ).all()
    return results


def update_salespersons(session, lead_id, dealer_partner_id, new_salespersons):
    """Update assigned salespersons for a given lead ID."""
    crm_ids = [new_person["crm_salesperson_id"] for new_person in new_salespersons if new_person.get("crm_salesperson_id")]
    db_salespersons = session.query(
        Salesperson
    ).filter(
        Salesperson.dealer_integration_partner_id == dealer_partner_id,
        Salesperson.crm_salesperson_id.in_(crm_ids)
    ).all()

    crm_to_db_salesperson = {db_person.crm_salesperson_id: db_person for db_person in db_salespersons}

    for new_person in new_salespersons:
        db_person = crm_to_db_salesperson.get(new_person["crm_salesperson_id"])
        if db_person:
            # Update existing salesperson
            db_person.first_name = new_person.get("first_name", "")
            db_person.last_name = new_person.get("last_name", "")
            db_person.phone = new_person.get("phone", "")
            db_person.email = new_person.get("email", "")
            db_person.position_name = new_person.get("position_name", "")
            new_person["salesperson_id"] = db_person.id
            logger.info(f"Updated Salesperson for lead_id {lead_id}, {new_person}")
        else:
            # Create new salesperson
            salesperson = Salesperson(
                crm_salesperson_id=new_person["crm_salesperson_id"],
                first_name=new_person.get("first_name", ""),
                last_name=new_person.get("last_name", ""),
                phone=new_person.get("phone", ""),
                email=new_person.get("email", ""),
                position_name=new_person.get("position_name", ""),
                dealer_integration_partner_id=dealer_partner_id
            )
            session.add(salesperson)
            session.flush()
            new_person.update({"salesperson_id": salesperson.id})
            logger.info(f"Created Salesperson for lead_id {lead_id}, {new_person}")
    session.commit()


def update_lead_salespersons(session, lead_id, dealer_partner_id, new_salespersons):
    """Update assigned lead salespersons for a given lead ID."""
    crm_ids = [new_person["salesperson_id"] for new_person in new_salespersons]
    db_lead_salespeople = session.query(
        Lead_Salesperson
    ).filter(
        Lead_Salesperson.lead_id == lead_id,
        Lead_Salesperson.salesperson_id.in_(crm_ids)
    ).all()

    crm_to_lead_salesperson = {ls.salesperson_id: ls for ls in db_lead_salespeople}

    for new_person in new_salespersons:
        is_primary = new_person.get("is_primary", False)

        lead_salesperson = crm_to_lead_salesperson.get(new_person["salesperson_id"])
        if lead_salesperson:
            # Update existing lead salesperson
            if lead_salesperson.is_primary != is_primary:
                lead_salesperson.is_primary = is_primary
                logger.info(f"Updated Lead_Salesperson for lead_id {lead_id}, {new_person}")
        else:
            # Create new lead salesperson
            lead_salesperson = Lead_Salesperson(
                lead_id=lead_id,
                salesperson_id=new_person["salesperson_id"],
                is_primary=is_primary
            )
            session.add(lead_salesperson)
            logger.info(f"Created Lead_Salesperson for lead_id {lead_id}, {new_person}")
    session.commit()


def modify_salespersons(session, lead_id, dealer_partner_id, new_salespersons):
    """Modify salesperson tables for a given lead ID."""
    if not new_salespersons:
        return

    update_salespersons(session, lead_id, dealer_partner_id, new_salespersons)
    update_lead_salespersons(session, lead_id, dealer_partner_id, new_salespersons)

    # Find lead_salespersons that are no longer assigned to the lead.
    removed_salespeople = []
    existing_salespersons = get_salespersons_for_lead(session, lead_id)
    for existing_person in existing_salespersons:
        if existing_person.salesperson_id not in [new_person["salesperson_id"] for new_person in new_salespersons]:
            removed_salespeople.append(existing_person.salesperson_id)

    # Delete lead_salesperson records that are no longer assigned to the lead.
    if removed_salespeople:
        session.query(Lead_Salesperson).filter(
                Lead_Salesperson.lead_id == lead_id,
                Lead_Salesperson.salesperson_id.in_(removed_salespeople)
            ).delete(synchronize_session=False)
        logger.info(f"Deleted Lead_Salesperson for lead_id {lead_id}, {removed_salespeople}")

    logger.info(f"Salespersons are updated {lead_id}")
    session.commit()


def lambda_handler(event: Any, context: Any) -> Any:
    """Update lead."""
    logger.info(f"Event: {event}")

    integration_partner = event["requestContext"]["authorizer"]["integration_partner"]
    lead_id = event["pathParameters"]["lead_id"]

    try:
        body = loads(event["body"])
        vehicles_of_interest = body.get('vehicles_of_interest', [])
        metadata = body.get('metadata')
        consumer_id = body.get("consumer_id")
        crm_lead_id = body.get("crm_lead_id")

        lead_field_mapping = {
            "lead_status": "status",
            "lead_substatus": "substatus",
            "lead_comment": "comment",
            "lead_origin": "origin_channel",
            "lead_source": "source_channel",
            "lead_source_detail": "source_detail",
        }

        with DBSession() as session:
            db_query = session.query(
                Lead, Consumer, DealerIntegrationPartner
            ).join(
                Consumer, Lead.consumer_id == Consumer.id
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Lead.id == lead_id
            )

            if integration_partner:
                db_query = db_query.filter(IntegrationPartner.impel_integration_partner_name == integration_partner)

            db_results = db_query.first()
            if not db_results:
                logger.error(f"Lead not found {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead not found {lead_id}"})
                }

            lead_db, consumer_db, dip_db = db_results

            if not any([dip_db.is_active, dip_db.is_active_salesai, dip_db.is_active_chatai]):
                error_msg = f"Dealer integration partner {dip_db.id} is not active. Lead failed to be updated."
                logger.error(error_msg)
                send_alert_notification(subject=f'CRM API: Lead update failure', message=error_msg)

                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Dealer integration partner {dip_db.id} is not active. Lead failed to be created."})
                }

            old_crm_lead_id = lead_db.crm_lead_id
            if crm_lead_id and crm_lead_id != old_crm_lead_id:
                logger.info(f"Updating crm_lead_id from {old_crm_lead_id} to {crm_lead_id}")
                lead_db.crm_lead_id = crm_lead_id

            dealer_partner_id = consumer_db.dealer_integration_partner_id
            if consumer_id and lead_db.consumer_id != consumer_id:
                consumer_new_db = session.query(Consumer).filter(Consumer.id == consumer_id).first()

                if not consumer_new_db:
                    logger.error(f"Consumer {consumer_id} not found and cannot be updated.")
                    return {
                        "statusCode": 404,
                        "body": dumps({"error": f"Consumer {consumer_id} not found and cannot be updated."})
                    }

                if consumer_db.dealer_integration_partner_id != consumer_new_db.dealer_integration_partner_id:
                    message = "Leads cannot be reassigned to consumers outside of their dealer_integration_partner."
                    logger.error(message)
                    return {
                        "statusCode": 400,
                        "body": dumps({"error": message})
                    }

                original_consumer = consumer_db.id
                lead_db.consumer_id = consumer_id
                logger.info(f"Lead reassigned from consumer {original_consumer} to {consumer_id}")

                dealer_partner_id = consumer_new_db.dealer_integration_partner_id

            # Update lead fields if provided
            for received_field, db_field in lead_field_mapping.items():
                if received_field in body:
                    new_value = body[received_field]
                    setattr(lead_db, db_field, new_value)
            # Update metadata if provided
            if metadata:
                if lead_db.metadata_:
                    lead_db.metadata_.update(metadata)
                else:
                    lead_db.metadata_ = metadata

            # Add new vehicles if provided
            for vehicle in vehicles_of_interest:
                vehicle = Vehicle(
                    lead_id=lead_db.id,
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
                    trade_in_vin=vehicle.get("trade_in_vin"),
                    trade_in_year=vehicle.get("trade_in_year"),
                    trade_in_make=vehicle.get("trade_in_make"),
                    trade_in_model=vehicle.get("trade_in_model"),
                    metadata_=vehicle.get("metadata")
                )
                session.add(vehicle)

            session.commit()
            logger.info(f"Lead is updated {lead_id}")

            modify_salespersons(session, lead_id, dealer_partner_id, body.get("salespersons", []))

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