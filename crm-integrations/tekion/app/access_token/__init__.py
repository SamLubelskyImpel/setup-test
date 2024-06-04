import logging
from os import environ

from .exceptions import TokenStillValid
from .s3 import get_token_from_s3
from .secrets import get_credentials_from_secrets
from .token_wrapper import TekionTokenWrapper

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def create_wrapper() -> TekionTokenWrapper:
    creds = get_credentials_from_secrets()
    last_token = get_token_from_s3()
    wrapper = TekionTokenWrapper(credentials=creds, token=last_token)
    return wrapper


def lambda_handler(event, context):
    """Handles Token Rotation Function."""
    try:
        wrapper = create_wrapper()
        try:
            wrapper.renew()
            wrapper.save()
        except TokenStillValid:
            logger.info("Token still valid, skipping renewal.")
    except Exception as e:
        logger.exception(str(e))
        raise
