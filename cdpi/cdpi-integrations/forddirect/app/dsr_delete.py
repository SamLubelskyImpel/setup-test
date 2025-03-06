import logging
from os import environ
from json import dumps
from utils import create_audit_dsr, call_events_api, send_alert_notification, log_dev
from uuid import uuid4

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.consumer_profile import ConsumerProfile
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.product import Product
from cdpi_orm.models.integration_partner import IntegrationPartner

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def validate_data(event):
    if "ext_consumer_id" not in event:
        return False, "ext_consumer_id is required"

    if "dealer_identifier" not in event:
        return False, "dealer_identifier is required"

    return True, ""

def lambda_handler(event, context):
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info("dsr_delete starting")
    log_dev(event)

    data_validated,error_msg = validate_data(event)
    
    if not data_validated:
        logger.error(f"Error invoking ford direct dsr optout. Response: {error_msg}")
        
        return {
            "statusCode": 400,
            "body": dumps(
                {
                    "message": error_msg
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }
    
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
                    Consumer.id == consumer_id
                ).first()

            if not consumer_db:
                logger.error(f"Consumer/Profile not found for consumer_id {consumer_id} and dealer_id {dealer_id}")

                return {
                    "statusCode": 404,
                    "body": dumps(
                        {
                            "message": f"Consumer/Profile not found for consumer_id {consumer_id} and dealer_id {dealer_id}"
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }
            
            consumer_profile, consumer, dealer, product, integration_partner = consumer_db

            session.delete(consumer_profile)

            audit_dsr = create_audit_dsr(integration_partner.id, consumer_id, event_type, complete_date=None, complete_flag=False)

            session.add(audit_dsr)
            session.commit()

            call_events_api(event_type, integration_partner.id, consumer_id, consumer.source_consumer_id, dealer_id,
                           dealer.salesai_dealer_id, dealer.serviceai_dealer_id, product.product_name)
            
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

        logger.exception(f"Error invoking ford direct dsr delete. Response: {e}")
        
        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "message": "Internal Server Error. Please contact Impel support."
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }