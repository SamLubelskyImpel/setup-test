"""Runs the vehicle sale API"""
import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.vehicle import Vehicle
from dms_orm.models.vehicle_sale import VehicleSale
from dms_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

STATEMENT_TIMEOUT = int(environ.get("STATEMENT_TIMEOUT_MS"))

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def convert_dt(value):
    """ Convert date or datetime string into datetime object """
    if "T" in value:
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
    else:
        return datetime.strptime(value, '%Y-%m-%d')


def lambda_handler(event, context):
    """Run vehicle sale API."""
    logger.info(f"Event: {event}")
    try:
        filters = event.get("queryStringParameters", {})
        page = 1 if not filters else int(filters.get("page", "1"))
        results = []
        max_results = 1000
        result_count = (
            max_results
            if not filters
            else int(filters.get("result_count", max_results))
        )
        max_results = min(max_results, result_count)

        with DBSession() as session:
            session.execute(text(f'SET LOCAL statement_timeout = {STATEMENT_TIMEOUT};'))
            query = (
                session.query(
                    VehicleSale,
                    Consumer,
                    DealerIntegrationPartner,
                    Dealer,
                    IntegrationPartner,
                    Vehicle,
                )
                .outerjoin(Consumer, VehicleSale.consumer_id == Consumer.id)
                .outerjoin(
                    DealerIntegrationPartner,
                    VehicleSale.dealer_integration_partner_id
                    == DealerIntegrationPartner.id,
                )
                .outerjoin(Dealer, DealerIntegrationPartner.dealer_id == Dealer.id)
                .outerjoin(
                    IntegrationPartner,
                    DealerIntegrationPartner.integration_partner_id
                    == IntegrationPartner.id,
                )
                .outerjoin(
                    Vehicle,
                    VehicleSale.vehicle_id == Vehicle.id,
                )
            )
            if filters:
                tables = [
                    VehicleSale,
                    Consumer,
                    DealerIntegrationPartner,
                    Dealer,
                    IntegrationPartner,
                    Vehicle,
                ]
                for attr, value in filters.items():
                    if attr == "sale_date_start":
                        query = query.filter(getattr(VehicleSale, "sale_date") >= convert_dt(value))
                    elif attr == "sale_date_end":
                        query = query.filter(getattr(VehicleSale, "sale_date") <= convert_dt(value))
                    elif attr == "db_creation_date_start":
                        query = query.filter(
                            getattr(VehicleSale, "db_creation_date") >= convert_dt(value)
                        )
                    elif attr == "db_creation_date_end":
                        query = query.filter(
                            getattr(VehicleSale, "db_creation_date") <= convert_dt(value)
                        )
                    else:
                        filtered_table = None
                        for table in tables:
                            if attr in table.__table__.columns:
                                filtered_table = table

                        if not filtered_table:
                            continue
                        query = query.filter(getattr(filtered_table, attr) == value)

            vehicle_sales = (
                query.order_by(VehicleSale.id)
                .limit(max_results + 1)
                .offset((page - 1) * max_results)
                .all()
            )

            results = []
            for (
                vehicle_sale,
                consumer,
                dealer_integration_partner,
                dealer,
                integration_partner,
                vehicle,
            ) in vehicle_sales[:max_results]:
                result_dict = vehicle_sale.as_dict()
                result_dict["consumer"] = consumer.as_dict()
                result_dict[
                    "dealer_integration_partner"
                ] = dealer_integration_partner.as_dict()
                result_dict["dealer"] = dealer.as_dict()
                result_dict["integration_partner"] = integration_partner.as_dict()
                result_dict["vehicle"] = vehicle.as_dict() if vehicle else None
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
                    "has_next_page": len(vehicle_sales) > max_results,
                },
                default=json_serial,
            ),
        }

    except Exception as e:
        logger.exception("Error running vehicle sale api.")
        raise
