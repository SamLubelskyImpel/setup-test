"""Runs the vehicle sale API"""
import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.vehicle import Vehicle
from dms_orm.models.vehicle_sale import VehicleSale
from dms_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def lambda_handler(event, context):
    """Run vehicle sale API."""
    logger.info(f"Event: {event}")
    try:
        filters = event.get("queryStringParameters", {})
        results = []
        max_results = 1000

        with DBSession() as session:
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
                    DealerIntegrationPartner.integration_id == IntegrationPartner.id,
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
                    Vehicle
                ]
                for attr, value in filters.items():
                    filtered_table = None
                    for table in tables:
                        if attr in table.__table__.columns:
                            filtered_table = table
                    
                    if not filtered_table:
                        raise RuntimeError(f"No column found {attr}")

                    if attr == "sale_date_start":
                        query = query.filter(getattr(filtered_table, "sale_date") >= value)
                    elif attr == "ro_open_date_end":
                        query = query.filter(getattr(filtered_table, "sale_date") <= value)
                    elif attr == "next_fetch_key":
                        query = query.filter(getattr(filtered_table, "id") > value)
                    elif attr == "result_count":
                        max_results = value
                    else:
                        query = query.filter(getattr(filtered_table, attr) == value)

            vehicle_sales = query.order_by(VehicleSale.id).limit(max_results).all()

            results = []
            for (
                vehicle_sale,
                consumer,
                dealer_integration_partner,
                dealer,
                integration_partner,
                vehicle,
            ) in vehicle_sales:
                result_dict = vehicle_sale.as_dict()
                result_dict["consumer"] = consumer.as_dict()
                result_dict[
                    "dealer_integration_partner"
                ] = dealer_integration_partner.as_dict()
                result_dict["dealer"] = dealer.as_dict()
                result_dict["integration_partner"] = integration_partner.as_dict()
                result_dict["vehicle"] = vehicle.as_dict() if vehicle else None
                results.append(result_dict)

        next_fetch_key = None

        if len(results) and len(results) == int(max_results):
            next_fetch_key = results[-1]["id"]

        return {
            "statusCode": "200",
            "body": dumps(
                {
                    "received_date_utc": datetime.utcnow()
                    .replace(microsecond=0)
                    .replace(tzinfo=timezone.utc)
                    .isoformat(),
                    "vehicle_sales": results,
                    "next_fetch_key": next_fetch_key,
                },
                default=json_serial,
            ),
        }

    except Exception:
        logger.exception("Error running vehicle sale api.")
        raise
