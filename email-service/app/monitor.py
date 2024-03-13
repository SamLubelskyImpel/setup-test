import logging
from os import environ

LOGLEVEL = environ.get('LOGLEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(LOGLEVEL))

def lambda_handler(event, _):
    ses_event = event['Records'][0]['Sns']['Message']
    logger.info(ses_event)