"""Runs the dealer API"""
import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def lambda_handler(event, context):
    """Run dealer API."""
    logger.info(f"Event: {event}")
    try:
        filters = event.get("queryStringParameters", {})
        page = 1 if not filters else int(filters.get("page", "1"))
        results = []
        max_results = 1000
        result_count = max_results if not filters else int(filters.get("result_count", max_results))
        max_results = min(max_results, result_count)
        

        with DBSession() as session:
            query = (
                session.query(DealerIntegrationPartner, Dealer, IntegrationPartner)
                .outerjoin(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
                .outerjoin(
                    IntegrationPartner,
                    DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id,
                )
            )

            if filters:
                tables = [DealerIntegrationPartner, Dealer, IntegrationPartner]
                for attr, value in filters.items():
                    filtered_table = None
                    for table in tables:
                        if attr in table.__table__.columns:
                            filtered_table = table
                
                    if not filtered_table:
                        continue

                    for attr, value in filters.items():
                        if attr == "db_creation_date_start":
                            query = query.filter(
                                getattr(DealerIntegrationPartner, "db_creation_date")
                                >= value
                            )
                        elif attr == "db_creation_date_end":
                            query = query.filter(
                                getattr(DealerIntegrationPartner, "db_creation_date")
                                <= value
                            )
                        else:
                            query = query.filter(getattr(filtered_table, attr) == value)

            dealers = (
                query.order_by(DealerIntegrationPartner.db_creation_date)
                .limit(max_results + 1)
                .offset((page - 1) * max_results)
                .all()
            )

            results = []
            for dealer_integration_partner, dealer, integration_partner in dealers[:max_results]:
                result_dict = dealer_integration_partner.as_dict()
                result_dict["dealer"] = dealer.as_dict()
                result_dict["integration_partner"] = integration_partner.as_dict()
                results.append(result_dict)

        return {
            "statusCode": "200",
            "body": dumps(
                {
                    "received_date_utc": datetime.utcnow()
                    .replace(microsecond=0)
                    .replace(tzinfo=timezone.utc)
                    .isoformat(),
                    "results": results,
                    "has_next_page": len(results) > max_results,
                },
                default=json_serial,
            ),
        }
    except Exception:
        logger.exception("Error running dealer api.")
        raise
