"""Runs the repair order API"""
import logging
from datetime import date, datetime, timezone
from json import dumps
from os import environ

from dms_orm.models.consumer import Consumer
from dms_orm.models.dealer import Dealer
from dms_orm.models.dealer_integration_partner import DealerIntegrationPartner
from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.models.op_code import OpCode
from dms_orm.models.op_code_repair_order import OpCodeRepairOrder
from dms_orm.models.service_repair_order import ServiceRepairOrder
from dms_orm.models.vehicle import Vehicle
from dms_orm.session_config import DBSession
from sqlalchemy import func, text
from sqlalchemy.orm import aliased

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def build_query_filters(params):
    """Build appropriate filters for query."""

    filters = {}
    if params:
        for attr, value in params.items():
            filters[attr] = value

    return filters


def lambda_handler(event, context):
    """Run repair order API."""

    logger.info(f"Event: {event}")
    try:
        filters = build_query_filters(event["queryStringParameters"])
        results = []
        max_results = 1000

        with DBSession() as session:
            op_code_1 = aliased(OpCode)
            subquery = (
                session.query(
                    ServiceRepairOrder.id.label("id"),
                    func.jsonb_agg(text("op_code_1")).label("op_codes"),
                )
                .join(
                    OpCodeRepairOrder,
                    OpCodeRepairOrder.repair_order_id == ServiceRepairOrder.id,
                )
                .join(
                    op_code_1,
                    op_code_1.id == OpCodeRepairOrder.op_code_id,
                )
                .group_by(ServiceRepairOrder.id)
                .subquery()
            )

            query = (
                session.query(
                    ServiceRepairOrder,
                    Consumer,
                    DealerIntegrationPartner,
                    Dealer,
                    IntegrationPartner,
                    Vehicle,
                    subquery.c.op_codes,
                )
                .outerjoin(
                    Consumer,
                    ServiceRepairOrder.consumer_id == Consumer.id,
                )
                .outerjoin(
                    DealerIntegrationPartner,
                    ServiceRepairOrder.dealer_integration_partner_id
                    == DealerIntegrationPartner.id,
                )
                .outerjoin(
                    Dealer,
                    DealerIntegrationPartner.dealer_id == Dealer.id,
                )
                .outerjoin(
                    IntegrationPartner,
                    DealerIntegrationPartner.integration_id == IntegrationPartner.id,
                )
                .outerjoin(
                    Vehicle,
                    ServiceRepairOrder.vehicle_id == Vehicle.id,
                )
                .outerjoin(subquery, subquery.c.id == ServiceRepairOrder.id)
            )

            if filters:
                for attr, value in filters.items():
                    if attr == "ro_open_date_start":
                        query = query.filter(
                            getattr(ServiceRepairOrder, "ro_open_date") >= value
                        )
                    elif attr == "ro_open_date_end":
                        query = query.filter(
                            getattr(ServiceRepairOrder, "ro_open_date") <= value
                        )
                    elif attr == "ro_close_date_start":
                        query = query.filter(
                            getattr(ServiceRepairOrder, "ro_close_date") >= value
                        )
                    elif attr == "ro_close_date_end":
                        query = query.filter(
                            getattr(ServiceRepairOrder, "ro_close_date") <= value
                        )
                    elif attr == "next_fetch_key":
                        query = query.filter(getattr(ServiceRepairOrder, "id") > value)
                    elif attr == "result_count":
                        max_results = value
                    else:
                        query = query.filter(getattr(ServiceRepairOrder, attr) == value)

            service_repair_orders = (
                query.order_by(ServiceRepairOrder.id).limit(max_results).all()
            )
            results = []
            for (
                service_repair_order,
                consumer,
                dealer_integration_partner,
                dealer,
                integration_partner,
                vehicle,
                op_codes,
            ) in service_repair_orders:
                result_dict = service_repair_order.as_dict()
                result_dict["consumer"] = consumer.as_dict()
                result_dict[
                    "dealer_integration_partner"
                ] = dealer_integration_partner.as_dict()
                result_dict["dealer"] = dealer.as_dict()
                result_dict["integration_partner"] = integration_partner.as_dict()
                result_dict["vehicle"] = vehicle.as_dict() if vehicle else None
                result_dict["op_codes"] = op_codes
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
                    "repair_orders": results,
                    "next_fetch_key": next_fetch_key,
                },
                default=json_serial,
            ),
        }
    except Exception:
        logger.exception("Error running repair order api.")
        raise
