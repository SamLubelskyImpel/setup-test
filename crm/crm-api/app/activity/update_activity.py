"""Update activity."""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from utils import send_general_alert_notification

from crm_orm.models.activity import Activity
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.lead import Lead
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

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
            actvitiy_db = session.query(Activity, DealerIntegrationPartner
            ).join(
                Lead, Activity.lead_id == Lead.id,
            ).join(
                Consumer, Lead.consumer_id == Consumer.id
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).filter(
                Activity.id == activity_id,
            ).first()

            if not actvitiy_db:
                logger.error(f"Activity {activity_id} not found. Activity failed to be updated.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Activity {activity_id} not found. Activity failed to be updated."})
                }

            activity, dealer_partner_db = actvitiy_db

            if not all ([dealer_partner_db.is_active, dealer_partner_db.is_active_salesai, dealer_partner_db.is_active_chatai]):
                error_msg = f"Dealer integration partner {dealer_partner_db.id} is not active. Activity failed to be updated."
                logger.error(error_msg)
                send_general_alert_notification(subject=f'CRM API: Activity Syndication Failure - Dealer integration partner inactive', message=error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }
            
            # Update activity
            activity.crm_activity_id = crm_activity_id
            session.commit()

            logger.info(f"Request product: {request_product} has updated activity {activity_id}")

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
