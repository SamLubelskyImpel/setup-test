import logging

from .envs import LOGLEVEL
from .s3 import get_token_from_s3
from .secrets import get_credentials_from_secrets
from .token_wrapper import TekionTokenWrapper

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


def create_wrapper() -> TekionTokenWrapper:
    logger.debug("Creating Token Wrapper")
    creds = get_credentials_from_secrets()
    last_token = get_token_from_s3()

    wrapper = TekionTokenWrapper(credentials=creds, token=last_token)
    return wrapper


def lambda_handler(event, context):
    """Handles Token Rotation Function."""
    logger.info("Starting Token Rotation Function: %s, %s", event, context)

    try:
        wrapper = create_wrapper()
        wrapper.renew()
        wrapper.save()
    except Exception as e:
        logger.exception(str(e))
        raise
