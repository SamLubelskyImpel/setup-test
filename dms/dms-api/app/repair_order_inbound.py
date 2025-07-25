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
from sqlalchemy.exc import OperationalError

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


def filter_query(query, filters, tables):
    """Function filters the query based on filters."""

    for attr, value in filters.items():
        if attr == "ro_open_date_start":
            query = query.filter(
                getattr(ServiceRepairOrder, "ro_open_date") >= convert_dt(value)
            )
        elif attr == "ro_open_date_end":
            query = query.filter(
                getattr(ServiceRepairOrder, "ro_open_date") <= convert_dt(value)
            )
        elif attr == "ro_close_date_start":
            query = query.filter(
                getattr(ServiceRepairOrder, "ro_close_date") >= convert_dt(value)
            )
        elif attr == "ro_close_date_end":
            query = query.filter(
                getattr(ServiceRepairOrder, "ro_close_date") <= convert_dt(value)
            )
        elif attr == "db_creation_date_start":
            query = query.filter(
                getattr(ServiceRepairOrder, "db_creation_date") >= convert_dt(value)
            )
        elif attr == "db_creation_date_end":
            query = query.filter(
                getattr(ServiceRepairOrder, "db_creation_date") <= convert_dt(value)
            )
        else:
            filtered_table = None
            for table in tables:
                if attr in table.__table__.columns:
                    filtered_table = table

            if not filtered_table:
                continue
            query = query.filter(getattr(filtered_table, attr) == value)
    return query


def lambda_handler(event, context):
    """Run repair order API."""

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
                    ServiceRepairOrder,
                    Consumer,
                    DealerIntegrationPartner,
                    Dealer,
                    IntegrationPartner,
                    Vehicle,
                    func.jsonb_agg(text("op_code.*")).label("op_codes")
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
                    DealerIntegrationPartner.integration_partner_id
                    == IntegrationPartner.id,
                )
                .outerjoin(
                    Vehicle,
                    ServiceRepairOrder.vehicle_id == Vehicle.id,
                )
                .outerjoin(
                    OpCodeRepairOrder,
                    OpCodeRepairOrder.repair_order_id == ServiceRepairOrder.id,
                )
                .outerjoin(
                    OpCode,
                    OpCode.id == OpCodeRepairOrder.op_code_id,
                )
                .group_by(
                    ServiceRepairOrder.id,
                    Consumer.id,
                    DealerIntegrationPartner.id,
                    Dealer.id, IntegrationPartner.id,
                    Vehicle.id
                )
            )

            if filters:
                query = filter_query(query, filters, [
                        ServiceRepairOrder,
                        Consumer,
                        DealerIntegrationPartner,
                        Dealer,
                        IntegrationPartner,
                        Vehicle
                    ]
                )
            service_repair_orders = (
                query.order_by(ServiceRepairOrder.id)
                .limit(max_results + 1)
                .offset((page - 1) * max_results)
                .all()
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
            ) in service_repair_orders[:max_results]:
                result_dict = service_repair_order.as_dict()
                result_dict["consumer"] = consumer.as_dict()
                result_dict[
                    "dealer_integration_partner"
                ] = dealer_integration_partner.as_dict()
                result_dict["dealer"] = dealer.as_dict()
                result_dict["integration_partner"] = integration_partner.as_dict()
                result_dict["vehicle"] = vehicle.as_dict() if vehicle else None
                result_dict["op_codes"] = [x for x in op_codes if x]
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
                    "has_next_page": len(service_repair_orders) > max_results,
                },
                default=json_serial,
            ),
        }

    except Exception as e:
        logger.exception("Error running repair order api.")
        raise
