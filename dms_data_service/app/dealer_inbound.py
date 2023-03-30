"""Runs the dealer API"""
import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ
from sqlalchemy import desc
import sys


logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())

from dms_orm.models.dealer import Dealer
from dms_orm.session_config import DBSession


def build_query_filters(params):
    """Build appropriate filters for query."""

    filters = {}
    if params:
        for attr, value in params.items():
            filters[attr] = value

    return filters


def lambda_handler(event, context):
    """Run dealer API."""
    logger.info(f'Event: {event}')
    try:

        filters = build_query_filters(event['queryStringParameters'])
        results = []
        max_results = 1000

        with DBSession() as session:

            query = session.query(Dealer)
            if filters:
                for attr, value in filters.items():
                    if attr == 'next_fetch_key':
                        query = query.filter(getattr(Dealer, 'id') > value)
                    elif attr == 'result_count':
                        max_results = value
                    else:
                        query = query.filter(getattr(Dealer, attr) == value)

            dealers = query.order_by(Dealer.id).limit(max_results).all()
            for dealer in dealers:
                results.append(dealer.as_dict())

        next_fetch_key = None

        if len(results) and len(results) == int(max_results):
            next_fetch_key = results[-1]['id']



        return {
            'statusCode': '200',
            'body': dumps({
                'received_date_utc': datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat(),
                'dealers': results,
                'next_fetch_key': next_fetch_key
            }, default=str)
        }
    except Exception:
        logger.exception('Error running dealer api.')
        raise
