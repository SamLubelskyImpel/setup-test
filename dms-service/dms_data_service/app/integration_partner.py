"""Runs the integration partner API"""
import logging
import sys
from datetime import datetime, timezone
from json import dumps, loads
from os import environ

from sqlalchemy import desc

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

from dms_orm.models.integration_partner import IntegrationPartner
from dms_orm.session_config import DBSession


def build_query_filters(params):
    """Build appropriate filters for query."""

    filters = {}
    if params:
        for attr, value in params.items():
            filters[attr] = value

    return filters


def lambda_handler(event, context):
    """Run integration partner API."""
    logger.info(f"Event: {event}")
    try:
        filters = build_query_filters(event["queryStringParameters"])
        results = []
        max_results = 1000

        with DBSession() as session:
            query = session.query(IntegrationPartner)
            if filters:
                for attr, value in filters.items():
                    if attr == "next_fetch_key":
                        query = query.filter(getattr(IntegrationPartner, "id") > value)
                    elif attr == "result_count":
                        max_results = value
                    else:
                        query = query.filter(getattr(IntegrationPartner, attr) == value)

            integration_partners = (
                query.order_by(IntegrationPartner.id).limit(max_results).all()
            )
            results = [
                integration_partner.as_dict()
                for integration_partner in integration_partners
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
                    "integration_partners": results,
                    "next_fetch_key": next_fetch_key,
                },
                default=str,
            ),
        }
    except Exception:
        logger.exception("Error running integration partner api.")
        raise
