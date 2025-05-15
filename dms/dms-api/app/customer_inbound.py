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

from sqlalchemy import text, or_

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

STATEMENT_TIMEOUT = int(environ.get("STATEMENT_TIMEOUT_MS") or 20000)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def lambda_handler(event, context):
    """Run customer API."""
    logger.info(f"Event: {event}")
    try:
        query_params = event.get("queryStringParameters")

        customer_id = query_params.get("consumer_id")
        email = query_params.get("email")
        phone = query_params.get("phone")

        with DBSession() as session:
            session.execute(text(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT};"))

            query = (
                session.query(
                    Consumer,
                    Dealer.id.label("dealer_id"),
                    Dealer.impel_dealer_id,
                    IntegrationPartner.id.label("integration_partner_id"),
                    IntegrationPartner.impel_integration_partner_id,
                )
                .join(
                    DealerIntegrationPartner,
                    Consumer.dealer_integration_partner_id
                    == DealerIntegrationPartner.id,
                )
                .outerjoin(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
                .outerjoin(
                    IntegrationPartner,
                    DealerIntegrationPartner.integration_partner_id
                    == IntegrationPartner.id,
                )
                .filter(Consumer.dealer_customer_no == customer_id)
            )

            if email:
                query = query.filter(Consumer.email.ilike(email))
            if phone:
                query = query.filter(
                    or_(Consumer.cell_phone == phone, Consumer.home_phone == phone)
                )

            result = query.all()

            if not result:
                return {
                    "statusCode": 404,
                    "body": dumps(
                        {
                            "error": f"Customer ID {customer_id} does not exist in the Shared Layer"
                        }
                    ),
                }

            # chech if the same customer was found for multiple dealers
            found_dealers = set([c[1] for c in result])
            if len(found_dealers) > 1:
                return {
                    "statusCode": 400,
                    "body": dumps(
                        {
                            "error": f"Multiple records found for Customer ID {customer_id}"
                        }
                    ),
                }

            (
                consumer,
                dealer_id,
                impel_dealer_id,
                integration_partner_id,
                impel_integration_partner_id,
            ) = result[0]

            result_dict = {
                **consumer.as_dict(),
                "dealer": {
                    "id": dealer_id,
                    "impel_dealer_id": impel_dealer_id,
                },
                "integration_partner": {
                    "id": integration_partner_id,
                    "impel_integration_partner_id": impel_integration_partner_id,
                },
            }

            return {
                "statusCode": 200,
                "body": dumps(
                    {
                        "received_date_utc": datetime.utcnow()
                        .replace(microsecond=0)
                        .replace(tzinfo=timezone.utc)
                        .isoformat(),
                        "results": [result_dict],
                    },
                    default=json_serial,
                ),
            }

    except Exception:
        logger.exception("Error running customer api.")
        raise
