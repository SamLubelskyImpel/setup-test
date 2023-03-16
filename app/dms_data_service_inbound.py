"""Runs the dms data service API"""
import logging
from datetime import datetime, timezone
from json import loads, dumps
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get('LOGLEVEL', 'INFO').upper())


def lambda_handler(event, context):
    """Run dms data service API."""
    logger.info(f'Event: {event}')
    try:
        message_body = loads(event['body'])
        headers = event['headers']

        logger.info(f'Received body {message_body} and headers {headers}')

        return {
            'statusCode': '200',
            'body': dumps({
                'received_date_utc': datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc).isoformat()
            })
        }
    except Exception:
        logger.exception('Error running dms data service api.')
        raise
