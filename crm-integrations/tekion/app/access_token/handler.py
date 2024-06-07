from aws_lambda_powertools.logging import Logger

from .s3 import get_token_from_s3
from .secrets import get_credentials_from_secrets
from .token_wrapper import TekionTokenWrapper

logger = Logger()


def create_wrapper() -> TekionTokenWrapper:
    logger.debug("Creating Token Wrapper")
    creds = get_credentials_from_secrets()
    last_token = get_token_from_s3()

    wrapper = TekionTokenWrapper(credentials=creds, token=last_token)
    return wrapper


def lambda_handler(event, context):
    """Handles Token Rotation Function."""
    logger.info("Starting Token Rotation Function", extra={"event": event})

    try:
        wrapper = create_wrapper()
        wrapper.renew()
        wrapper.save()

        logger.info("Token Rotation Function completed successfully")

    except Exception as e:
        logger.exception(str(e))
        raise
