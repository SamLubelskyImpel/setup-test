"""Update activity."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from .utils import send_general_alert_notification
from common.utils import get_secret, send_message_to_event_enricher

from crm_orm.models.activity import Activity
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession
from crm_orm.models.dealer import Dealer

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Update activity."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        activity_id = event["pathParameters"]["activity_id"]

        crm_activity_id = body["crm_activity_id"]

        with DBSession() as session:
            # Check activity existence
            actvitiy_db = session.query(Activity, DealerIntegrationPartner, Dealer.idp_dealer_id
            ).join(
                Lead, Activity.lead_id == Lead.id,
            ).join(
                Consumer, Lead.consumer_id == Consumer.id
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).filter(
                Activity.id == activity_id,
            ).first()

            if not actvitiy_db:
                logger.error(f"Activity {activity_id} not found. Activity failed to be updated.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Activity {activity_id} not found. Activity failed to be updated."})
                }

            activity, dealer_partner_db, idp_dealer_id = actvitiy_db
            lead_id = activity.lead_id

            if not any([dealer_partner_db.is_active, dealer_partner_db.is_active_salesai, dealer_partner_db.is_active_chatai]):
                error_msg = f"Dealer integration partner {dealer_partner_db.id} is not active. Activity failed to be updated."
                logger.error(error_msg)
                send_general_alert_notification(subject=f'CRM API: Activity update failure', message=error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }

            # Update activity
            activity.crm_activity_id = crm_activity_id
            session.commit()

            logger.info(f"Request product: {request_product} has updated activity {activity_id}")

        secret = get_secret("crm-api", request_product)
        source_application = 'INTEGRATION'
        if secret.get("source_application"):
            source_application = secret["source_application"]

        payload_details = {
            "lead_id": lead_id,
            "activity_id": activity_id,
            "source_application": source_application,
            "idp_dealer_id": idp_dealer_id,
            "event_type": "Activity Updated",
        }

        send_message_to_event_enricher(payload_details)

        return {
            "statusCode": 200,
            "body": dumps({"message": "Activity updated successfully"})
        }

    except Exception as e:
        logger.error(f"Error updating activity: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
