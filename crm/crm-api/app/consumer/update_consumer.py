"""Update consumer."""

import logging
from os import environ
from json import loads, dumps
from typing import Any
from utils import send_alert_notification

from crm_orm.models.consumer import Consumer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.session_config import DBSession

from utils import get_restricted_query

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Update consumer."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        consumer_id = event["pathParameters"]["consumer_id"]
        integration_partner = event["requestContext"]["authorizer"]["integration_partner"]

        fields_to_update = [
            "crm_consumer_id", "first_name", "last_name", "middle_name", "email", "phone",
            "email_optin_flag", "sms_optin_flag", "city", "country",
            "address", "postal_code"
        ]

        with DBSession() as session:
            consumer_query = session.query(
                Consumer, DealerIntegrationPartner
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Consumer.id == consumer_id
            )

            consumer_query = get_restricted_query(consumer_query, integration_partner)
            consumer_db = consumer_query.first()

            if not consumer_db:
                logger.error(f"Consumer {consumer_id} not found")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Consumer {consumer_id} not found."})
                }
            
            consumer, dealer_partner_db = consumer_db

            if not any([dealer_partner_db.is_active, dealer_partner_db.is_active_salesai, dealer_partner_db.is_active_chatai]):
                error_msg = f"Dealer integration partner {dealer_partner_db.id} is not active. Consumer failed to be updated."
                logger.error(error_msg)
                send_alert_notification(subject=f'CRM API: Consumer update failure', message=error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }
            
            for field in fields_to_update:
                if field in body:
                    setattr(consumer, field, body[field])

            session.commit()

        logger.info(f"Consumer updated {consumer_id}")

        return {
            "statusCode": 200,
            "body": dumps({"message": "Consumer updated successfully"})
        }

    except Exception as e:
        logger.error(f"Error updating consumer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
