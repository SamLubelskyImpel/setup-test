"""Runs the vehicle sale API"""
import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())


def lambda_handler(event, context):
    """Run vehicle sale API."""
    logger.info(f'Event: {event}')
    try:
        return {
            'statusCode': '200',
            'body': dumps({
                'received_date_utc': datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat()
            })
        }
    except Exception:
        logger.exception('Error running vehicle sale api.')
        raise
