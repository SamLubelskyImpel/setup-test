import logging
from os import environ
from datetime import datetime
from json import dumps

from crm_orm.models.dealer import Dealer
from crm_orm.models.consumer import Consumer
from crm_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Create consumer."""
    logger.info(f"Event: {event}")

    body = event["body"]
    request_product = event["headers"]["client_id"]
    dealer_id = event["headers"]["dealer_id"]

    with DBSession as session:
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

        # Create consumer
        consumer = Consumer(
            dealer_id=dealer.id,
            integration_partner_id=dealer.integration_partner_id,
            first_name=body["first_name"],
            last_name=body["last_name"],
            middle_name=body("middle_name", ""),
            email=body["email"],
            phone=body.get("phone", ""),
            postal_code=body.get("postal_code", ""),
            address=body.get("address", ""),
            country=body.get("country", ""),
            city=body.get("city", ""),
            email_optin_flag=body["email_optin_flag"],
            sms_optin_flag=body["sms_optin_flag"],
            request_product=request_product,
            db_creation_date=datetime.utcnow(),
            db_update_date=datetime.utcnow(),
            db_update_role="system"
        )
        session.add(consumer)
        session.commit()

    logger.info(f"Created consumer {consumer.id}")

    return {
        "statusCode": "200",
        "body": dumps({"consumerId": consumer.id})
    }
