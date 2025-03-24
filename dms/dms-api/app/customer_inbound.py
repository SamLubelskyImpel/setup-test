"""Runs the customer API"""
import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.session_config import DBSession

from sqlalchemy import text, or_, func, distinct
from sqlalchemy.exc import OperationalError

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

STATEMENT_TIMEOUT = int(environ.get("STATEMENT_TIMEOUT_MS"))


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def lambda_handler(event, context):
    """Run customer API."""
    logger.info(f"Event: {event}")
    try:
        filters = event.get("queryStringParameters", {})
        customer_id = filters.get("customer_id", None)

        if not customer_id:
            return {"statusCode": 400, "body": dumps({"error": "Customer ID is required"})}

        with DBSession() as session:
            session.execute(text(f'SET LOCAL statement_timeout = {STATEMENT_TIMEOUT};'))

            id_exists = session.query(
                session.query(Consumer.id).filter(Consumer.id == customer_id).exists()
            ).scalar()

            dealer_no_count, partner_count = session.query(
                func.count(Consumer.dealer_customer_no),
                func.count(distinct(Consumer.dealer_integration_partner_id))
            ).filter(
                Consumer.dealer_customer_no == customer_id
            ).first()

            if id_exists and dealer_no_count > 0:
                return {
                    "statusCode": 400,
                    "body": dumps({"error": f"Customer ID {customer_id} exists as both Consumer ID and Dealer Customer No"})
                }
            if dealer_no_count > 0 and partner_count > 1:
                return {
                    "statusCode": 400,
                    "body": dumps({"error": f"Multiple dealer integration partners found for Customer ID {customer_id}"})
                }

            query = (
                session.query(
                    Consumer.id,
                    Consumer.dealer_customer_no,
                    Dealer.id.label("dealer_id"),
                    Dealer.impel_dealer_id,
                    IntegrationPartner.id.label("integration_partner_id"),
                    IntegrationPartner.impel_integration_partner_id
                ).join(
                    DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
                ).outerjoin(
                    Dealer, DealerIntegrationPartner.dealer_id == Dealer.id
                ).outerjoin(
                    IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
                )
            )

            if id_exists:
                consumer = query.filter(Consumer.id == customer_id).first()
            elif dealer_no_count > 0:
                consumer = query.filter(Consumer.dealer_customer_no == customer_id).first()
            else:
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Customer ID {customer_id} does not exist in the Shared Layer"})
                }

            result_dict = {
                "id": consumer.id,
                "dealer_customer_no": consumer.dealer_customer_no,
                "dealer": {
                    "id": consumer.dealer_id,
                    "impel_dealer_id": consumer.impel_dealer_id
                },
                "integration_partner": {
                    "id": consumer.integration_partner_id,
                    "impel_integration_partner_id": consumer.impel_integration_partner_id
                }
            }

            return {
                "statusCode": 200,
                "body": dumps(
                    {
                        "received_date_utc": datetime.utcnow()
                        .replace(microsecond=0)
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
                        "results": [result_dict]
                    },
                    default=json_serial,
                ),
            }

    except Exception as e:
        logger.exception("Error running customer api.")
        raise
