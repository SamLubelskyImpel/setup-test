import logging
from os import environ
from json import dumps
from utils import clear_consumer_profile, create_audit_dsr, create_event, send_alert_notification
from uuid import uuid4

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.consumer_profile import ConsumerProfile
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.product import Product
from cdpi_orm.models.integration_partner import IntegrationPartner

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    logger.info("dsr_delete starting")
    logger.info(event)

    try:
        consumer_id = event["ext_consumer_id"]
        dealer_id = event["dealer_identifier"]
        event_type = "cdp.dsr.delete"

        with DBSession() as session:
            consumer_db = session.query(
                    ConsumerProfile, Consumer, Dealer, Product, IntegrationPartner
                ).join(
                    Consumer, Consumer.id == ConsumerProfile.consumer_id
                ).join(
                    Dealer, Dealer.id == Consumer.dealer_id
                ).join(
                    Product, Product.id == Consumer.product_id
                ).join(
                    IntegrationPartner, IntegrationPartner.id == ConsumerProfile.integration_partner_id
                ).filter(
                    Consumer.dealer_id == dealer_id,
                    ConsumerProfile.consumer_id == consumer_id
                ).first()

            if not consumer_db:
                logger.error(f"Consumer not found for consumer_id {consumer_id} and dealer_id {dealer_id}")

                return {
                    "statusCode": 404,
                    "body": dumps(
                        {
                            "message": f"Consumer not found for consumer_id {consumer_id} and dealer_id {dealer_id}"
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }
            
            consumer_profile, consumer, dealer, product, integration_partner = consumer_db

            consumer_profile = clear_consumer_profile(consumer_profile)

            audit_dsr = create_audit_dsr(integration_partner.id, consumer_id, event_type, complete_date=None, complete_flag=False)

            session.add(audit_dsr)
            session.commit()

            create_event(integration_partner.id, consumer, dealer, product, event_type)
            
        logger.info("dsr_delete completed")
        return {
            "statusCode": 200,
            "body": dumps(
                {
                    "message": "Success"
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:
        send_alert_notification(request_id, event_type, e)

        logger.exception(f"Error invoking ford direct dsr delete response {e}")
        
        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "message": "Internal Server Error. Please contact Impel support."
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }