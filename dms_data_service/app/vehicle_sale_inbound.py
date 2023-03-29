"""Runs the vehicle sale API"""
import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ
from sqlalchemy import desc
import sys


logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())

from dms_orm.models.vehicle_sale import VehicleSale
from dms_orm.session_config import DBSession


def build_query_filters(params):
    """Build appropriate filters for query."""

    filters = {}
    if params:
        for attr, value in params.items():
            filters[attr] = value

    return filters


def lambda_handler(event, context):
    """Run vehicle sale API."""
    logger.info(f'Event: {event}')
    try:
        filters = build_query_filters(event['queryStringParameters'])
        results = []
        max_results = 1000

        with DBSession() as session:

            query = session.query(VehicleSale)
            if filters:
                for attr, value in filters.items():
                    if attr == 'sale_date_start':
                        query = query.filter(getattr(VehicleSale, 'sale_date') >= value)
                    elif attr == 'ro_open_date_end':
                        query = query.filter(getattr(VehicleSale, 'sale_date') <= value)
                    elif attr == 'next_fetch_key':
                        query = query.filter(getattr(VehicleSale, 'id') > value)
                    elif attr == 'result_count':
                        max_results = value
                    else:
                        query = query.filter(getattr(VehicleSale, attr) == value)

            vehicle_sales = query.order_by(VehicleSale.id).limit(max_results).all()
            for vehicle_sale in vehicle_sales:
                results.append(vehicle_sale.as_dict())

        next_fetch_key = None

        if len(results) and len(results) == int(max_results):
            next_fetch_key = results[-1]['id']


        return {
            'statusCode': '200',
            'body': dumps({
                'received_date_utc': datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat(),
                'results': results,
                'next_fetch_key': next_fetch_key
            }, default=str)
        }

    except Exception:
        logger.exception('Error running vehicle sale api.')
        raise
