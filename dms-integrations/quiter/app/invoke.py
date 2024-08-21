from os import environ
import logging


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, _):
    logger.info(f'Event: {event}')