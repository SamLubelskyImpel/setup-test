"""Create consumer."""

import logging
from os import environ
from json import dumps, loads
from typing import Any, List
from sqlalchemy import or_
from utils import send_alert_notification

from crm_orm.models.dealer import Dealer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession
from crm_orm.models.integration_partner import IntegrationPartner


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

consumer_attrs = ['dealer_integration_partner_id', 'crm_consumer_id', 'first_name', 'last_name', 'middle_name',
                  'email', 'phone', 'postal_code', 'address', 'country',
                  'city', 'email_optin_flag', 'sms_optin_flag', 'request_product']


def update_attrs(db_object: Any, data: Any, dealer_partner_id: str,
                 allowed_attrs: List[str], request_product) -> None:
    """Update attributes of a database object."""
    additional_attrs = {
        "email_optin_flag": data.get("email_optin_flag", True),
        "sms_optin_flag": data.get("sms_optin_flag", True),
        "request_product": request_product
    }

    combined_data = {"dealer_integration_partner_id": dealer_partner_id, **data, **additional_attrs}

    for attr in allowed_attrs:
        if attr in combined_data:
            setattr(db_object, attr, combined_data[attr])


def lambda_handler(event: Any, context: Any) -> Any:
    """Create consumer."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])
        request_product = event["headers"]["partner_id"]
        product_dealer_id = event["queryStringParameters"]["dealer_id"]
        authorizer_integration_partner = event["requestContext"]["authorizer"]["integration_partner"]

        crm_consumer_id = body.get("crm_consumer_id", None)
        created_consumer = True

        if not body.get("email") and not body.get("phone"):
            logger.error("Consumer must have an email or phone number.")
            return {
                "statusCode": 400,
                "body": dumps({"error": "Consumer must have an email or phone number."})
            }

        with DBSession() as session:
            db_results = session.query(
                DealerIntegrationPartner, IntegrationPartner.impel_integration_partner_name
            ).join(
                Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id
            ).filter(
                Dealer.product_dealer_id == product_dealer_id,
                or_(DealerIntegrationPartner.is_active.is_(True),
                    DealerIntegrationPartner.is_active_salesai.is_(True),
                    DealerIntegrationPartner.is_active_chatai.is_(True))
            ).first()
            if not db_results:
                error_msg = f"No active dealer found with id {product_dealer_id}. Consumer failed to be created."
                logger.error(error_msg)
                send_alert_notification(subject='CRM API: Consumer creation failure', message=error_msg)
                return {
                    "statusCode": 404,
                    "body": dumps({"error": error_msg})
                }

            dealer_partner, integration_partner_name = db_results
            if authorizer_integration_partner:
                if authorizer_integration_partner != integration_partner_name:
                    return {
                        "statusCode": 401,
                        "body": dumps(
                            {
                                "error": "This request is unauthorized. The authorization credentials are missing or are wrong. For example, the partner_id or the x_api_key provided in the header are wrong/missing."
                            }
                        ),
                    }

            # Query for existing consumer
            consumer_db = None
            if crm_consumer_id:
                consumer_db = session.query(
                    Consumer
                ).filter(
                    Consumer.crm_consumer_id == crm_consumer_id,
                    Consumer.dealer_integration_partner_id == dealer_partner.id
                ).first()

            if not consumer_db:
                logger.info(f"Consumer {crm_consumer_id} does not exist for dealer {dealer_partner.id}. Creating consumer.")
                consumer_db = Consumer()
            else:
                logger.info(f"Consumer {crm_consumer_id} already exists for dealer {dealer_partner.id}. Updating consumer.")
                created_consumer = False

            update_attrs(consumer_db, body, dealer_partner.id, consumer_attrs, request_product)
            if not consumer_db.id:
                session.add(consumer_db)

            session.commit()
            consumer_id = consumer_db.id

        if created_consumer:
            logger.info(f"Created consumer {consumer_id}")
            return {
                "statusCode": 201,
                "body": dumps({"consumer_id": consumer_id})
            }
        else:
            logger.info(f"Updated existing consumer {consumer_id}")
            return {
                "statusCode": 200,
                "body": dumps({"consumer_id": consumer_id})
            }

    except Exception as e:
        logger.error(f"Error creating consumer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
