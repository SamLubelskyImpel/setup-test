import logging
from os import environ
from json import dumps, loads, JSONDecodeError
from utils import send_alert_notification, log_dev, send_message_to_sqs, ValidationErrorResponse, sanitize_errors
from uuid import uuid4
from typing import Optional
from pydantic import BaseModel, Field, ValidationError

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.consumer import Consumer
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.product import Product

DSR_HANDLER_QUEUE_URL = environ.get("DSR_HANDLER_QUEUE_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class DsrDelete(BaseModel):
    ext_consumer_id: str = Field(
        ..., description="Consumer ID"
    )
    dealer_identifier: str = Field(
        ..., description="Dealer ID"
    )
    dsr_delete_request_id: str = Field(
        ..., description="DSR Request ID"
    )
    dsr_request_timestamp: str = Field(
        ..., description="DSR Request Timestamp"
    )
    is_enterprise: str = Field(
        ..., description="Enterprise flag"
    )
    pii_matched: Optional[list] = Field(
        [], description="PII of Consumer"
    )
    record_timestamp: Optional[str] = Field(
        "", description="Record Timestamp"
    )


def lambda_handler(event, context):
    event_type = "cdp.dsr.delete"
    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info("dsr_delete starting")
    log_dev(event)

    try:
        try:
            body = loads(event["body"])
            delete_obj = DsrDelete(**body)
        except JSONDecodeError as json_err:
            logger.error("JSON decoding error: %s", json_err, exc_info=True)
            sanitized = [
                {"field": "body", "message": "Invalid JSON format"}
            ]
            raise ValidationErrorResponse(sanitized, json_err)
        except ValidationError as e:
            logger.error("Validation error: %s", e, exc_info=True)
            sanitized = sanitize_errors(e.errors())
            raise ValidationErrorResponse(sanitized, e)

        consumer_id = delete_obj.ext_consumer_id
        dealer_id = delete_obj.dealer_identifier
        with DBSession() as session:
            consumer_query_db = session.query(
                    Consumer, Dealer, Product.product_name
                ).join(
                    Dealer, Dealer.id == Consumer.dealer_id
                ).join(
                    Product, Product.id == Consumer.product_id
                ).filter(
                    Consumer.dealer_id == dealer_id,
                    Consumer.id == consumer_id
                ).first()

            if not consumer_query_db:
                logger.warning(f"Consumer not found for consumer_id {consumer_id} and dealer_id {dealer_id}.")
                return {
                    "statusCode": 404,
                    "body": dumps(
                        {
                            "message": "Consumer not found"
                        }
                    ),
                    "headers": {"Content-Type": "application/json"},
                }

            consumer, dealer, product_name = consumer_query_db

            if product_name == 'Sales AI':
                source_dealer_id = dealer.salesai_dealer_id
            elif product_name == 'Service AI':
                source_dealer_id = dealer.serviceai_dealer_id

            data = {
                "consumer_id": consumer_id,
                "dealer_id": dealer_id,
                "source_dealer_id": source_dealer_id,
                "source_consumer_id": consumer.source_consumer_id,
                "product_name": product_name,
                "event_type": event_type,
                "dsr_request_id": delete_obj.dsr_delete_request_id,
                "dsr_request_timestamp": delete_obj.dsr_request_timestamp,
                "is_enterprise": delete_obj.is_enterprise
            }

            send_message_to_sqs(DSR_HANDLER_QUEUE_URL, data)

        return {
            "statusCode": 200,
            "body": dumps(
                {
                    "message": "Success"
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    except ValidationErrorResponse:
        return {
            "statusCode": 400,
            "body": dumps(
                {
                    "message": "Invalid request body."
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
