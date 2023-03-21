"""Runs the vehicle sale API"""
import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ
from sqlalchemy import desc
import sys


logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())

from dms_orm.models.consumer import Consumer
from dms_orm.session_config import DBSession


def build_query_filters(params):
    """Build appropriate filters for query."""

    filters = {}
    for attr, value in params.items():
        filters[attr] = value

    return filters


def lambda_handler(event, context):
    """Run vehicle sale API."""
    logger.info(f'Event: {event}')
    try:
        filters = build_query_filters(event['queryStringParameters'])

        with DBSession() as session:
            query = session.query(User)
            max_results = 1000

            for attr, value in filters.items():
                if attr == 'sale_date_start':
                    query = query.filter(getattr(User, attr) >= value)
                elif attr == 'ro_open_date_end':
                    query = query.filter(getattr(User, attr) <= value)
                elif attr = 'next_fetch_key':
                    query = query.filter(getattr(User, 'id') > value)
                elif attr == 'result_count':
                    max_results = value
                else:
                    query = query.filter(getattr(User, attr) == value)

            results = query.order_by(desc(User.id)).limit(max_results).all()

            # grab first record and return id as next_fetch_key

        return {
            'statusCode': '200',
            'body': dumps({
                'received_date_utc': datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat()
            })
        }
    except Exception:
        logger.exception('Error running vehicle sale api.')
        raise
