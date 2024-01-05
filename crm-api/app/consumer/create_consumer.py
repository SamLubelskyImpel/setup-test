"""Create consumer."""

import logging
from os import environ
from json import dumps, loads
from typing import Any, List

from crm_orm.models.dealer import Dealer
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

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

        crm_consumer_id = body.get("crm_consumer_id", None)
        created_consumer = True

        with DBSession() as session:
            dealer_partner = session.query(DealerIntegrationPartner).\
                join(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id).\
                filter(
                    Dealer.product_dealer_id == product_dealer_id,
                    DealerIntegrationPartner.is_active == True
                ).first()
            if not dealer_partner:
                logger.error(f"No active dealer found with id {product_dealer_id}. Consumer failed to be created.")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"No active dealer found with id {product_dealer_id}. Consumer failed to be created."})
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
                session.flush()

            session.commit()
            consumer_id = consumer_db.id

        if created_consumer:
            logger.info(f"Created consumer {consumer_id}")
            return {
                "statusCode": "201",
                "body": dumps({"consumer_id": consumer_id})
            }
        else:
            logger.info(f"Updated existing consumer {consumer_id}")
            return {
                "statusCode": "200",
                "body": dumps({"consumer_id": consumer_id})
            }

    except Exception as e:
        logger.error(f"Error creating consumer: {str(e)}")
        return {
            "statusCode": "500",
            "body": dumps({"error": "An error occurred while processing the request."})
        }
