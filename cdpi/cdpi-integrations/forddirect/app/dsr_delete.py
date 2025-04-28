import logging
from os import environ
from json import dumps, loads
from utils import create_audit_dsr, call_events_api, send_alert_notification, log_dev, send_message_to_sqs
from uuid import uuid4
from datetime import datetime

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.consumer_profile import ConsumerProfile
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.product import Product
from cdpi_orm.models.integration_partner import IntegrationPartner

FORD_DIRECT_QUEUE_URL = environ.get("FORD_DIRECT_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def validate_data(event):
    if "ext_consumer_id" not in event:
        return False, "ext_consumer_id is required"

    if "dealer_identifier" not in event:
        return False, "dealer_identifier is required"

    return True, ""


def lambda_handler(event, context):
    event_type = "cdp.dsr.delete"
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info("dsr_delete starting")
    log_dev(event)

    body = loads(event["body"])

    data_validated, error_msg = validate_data(body)

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
        consumer_id = body["ext_consumer_id"]
        dealer_id = body["dealer_identifier"]
        dsr_request_id = body["dsr_delete_request_id"]

        logger.info(f"consumer_id: {consumer_id}, dealer_id: {dealer_id}")

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
                logger.warning(f"Consumer/Profile not found for consumer_id {consumer_id} and dealer_id {dealer_id}. Skipping DSR event.")
                return {
                    "statusCode": 200,
                    "body": dumps(
                        {
                            "message": "Success"
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }

            consumer_profile, consumer, dealer, product, integration_partner = consumer_db

            # FD data only needs to be deleted if recorded prior to the delete request
            dsr_request_time = body["dsr_request_timestamp"]
            dt = datetime.fromisoformat(dsr_request_time.replace("Z", "+00:00"))
            old_event = False
            if consumer_profile.score_update_date:
                if consumer_profile.score_update_date > dt:
                    old_event = True
            elif consumer_profile.db_creation_date > dt:
                old_event = True

            # May have to generate a response to Ford still
            if old_event:
                logger.info("DSR Request Timestamp prior to latest score. Auto-completing delete request.")
                data = {
                    "consumer_id": consumer_id,
                    "dealer_id": dealer_id,
                    "event_type": event_type,
                    "dsr_request_id": dsr_request_id,
                    "completed_flag": True
                }
                send_message_to_sqs(str(FORD_DIRECT_QUEUE_URL), data)
                return {
                    "statusCode": 200,
                    "body": dumps(
                        {
                            "message": "Success"
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }

            session.delete(consumer_profile)

            audit_dsr = create_audit_dsr(
                integration_partner.id,
                consumer_id,
                event_type,
                dsr_request_id=dsr_request_id,
                complete_date=None,
                complete_flag=False
            )
            session.add(audit_dsr)

            call_events_api(event_type, integration_partner.id, consumer_id, consumer.source_consumer_id, dealer_id,
                            dealer.salesai_dealer_id, dealer.serviceai_dealer_id, product.product_name)

            session.commit()

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
