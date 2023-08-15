"""Insert unified repair order records."""
import logging
from json import loads
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: dict, context: dict):
    """ Insert unified repair order records into the DMS database. """
    try:
        for event in [e for e in event["Records"]]:
            message = loads(event["body"])
            logger.info(f"message of {message}")
    except Exception as e:
        logger.exception("Error inserting repair order DMS records")
        raise e
