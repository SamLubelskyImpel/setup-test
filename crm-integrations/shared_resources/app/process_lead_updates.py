"""Process lead updates."""

import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from crm_orm.models.lead import Lead
from crm_orm.models.lead_salesperson import Lead_Salesperson
from crm_orm.models.salesperson import Salesperson
from crm_orm.session_config import DBSession

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_salespersons_for_lead(lead_id: str) -> Any:
    """Retrieve all salespersons for a given lead ID."""
    with DBSession() as session:
        results = session.query(Salesperson, Lead_Salesperson.is_primary)\
            .join(
                Lead_Salesperson,
                Salesperson.id == Lead_Salesperson.salesperson_id,
            ).filter(Lead_Salesperson.lead_id == lead_id)\
            .all()
        return results


def update_salespersons(lead_id, updated_salespersons, dealer_id):
    """Update salespersons for a given lead ID."""
    to_delete = []

    # Update existing salespersons, filter out old salespersons
    for salesperson, is_primary in get_salespersons_for_lead(lead_id):
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
                            "crm_salesperson_id": person.get("crm_salesperson_id", ""),
                            "first_name": person.get("first_name", ""),
                            "last_name": person.get("last_name", ""),
                            "phone": person.get("phone", ""),
                            "email": person.get("email", ""),
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
                        Salesperson.crm_salesperson_id == person.get("crm_salesperson_id", "")
                    ).first()
                if salesperson:
                    salesperson.first_name = person.get("first_name", "")
                    salesperson.last_name = person.get("last_name", "")
                    salesperson.phone = person.get("phone", "")
                    salesperson.email = person.get("email", "")
                    salesperson.position_name = person.get("position_name", "")
                    session.commit()
                else:
                    # Create new salesperson
                    salesperson = Salesperson(
                        crm_salesperson_id=person.get("crm_salesperson_id", ""),
                        first_name=person.get("first_name", ""),
                        last_name=person.get("last_name", ""),
                        phone=person.get("phone", ""),
                        email=person.get("email", ""),
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


def update_lead_status(lead_id, status):
    # Update lead status if necessary
    with DBSession() as session:
        lead = session.query(
                Lead
            ).filter(
                Lead.id == lead_id
            ).first()

        if not lead:
            logger.error(f"Lead not found {lead_id}")
            raise

        if lead.status != status:
            logger.info(f"Updating lead status lead_id {lead_id} from {lead.status} to {status}")
            lead.status = status
            session.commit()
        else:
            logger.info(f"Lead status is already {status}")


def record_handler(record: SQSRecord):
    """Process lead updates."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record["body"])

        lead_id = body["lead_id"]
        dealer_id = body["dealer_id"]
        status = body.get("status", "")
        salespersons = body.get("salespersons", [])
        if salespersons:
            update_salespersons(lead_id, salespersons, dealer_id)

        if status:
            update_lead_status(lead_id, status)

    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Process lead updates."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
