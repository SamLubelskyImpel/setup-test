import logging
from os import environ

ENVIRONMENT = environ["ENVIRONMENT"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    logger.info(f"Event: {event}")
    pass
