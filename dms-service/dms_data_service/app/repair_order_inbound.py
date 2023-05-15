"""Runs the repair order API"""
import logging
import sys
from datetime import datetime, timezone
from json import dumps, loads
from os import environ

from sqlalchemy import desc

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

from dms_orm.models.service_repair_order import ServiceRepairOrder
from dms_orm.session_config import DBSession


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
            query = session.query(ServiceRepairOrder)

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
            results = [
                service_repair_order.as_dict()
                for service_repair_order in service_repair_orders
            ]

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
                default=str,
            ),
        }
    except Exception:
        logger.exception("Error running repair order api.")
        raise
