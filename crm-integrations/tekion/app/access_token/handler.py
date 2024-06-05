from os import environ

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from .exceptions import TokenStillValid
from .s3 import get_token_from_s3
from .secrets import get_credentials_from_secrets
from .token_wrapper import TekionTokenWrapper

logger = Logger()


def create_wrapper() -> TekionTokenWrapper:
    creds = get_credentials_from_secrets()
    last_token = get_token_from_s3()

    logger.debug(
        "Creating Token Wrapper",
        extra={
            "found_credentials": creds is not None,
            "has_last_token": last_token is not None,
        }
    )

    wrapper = TekionTokenWrapper(credentials=creds, token=last_token)
    return wrapper


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext):
    """Handles Token Rotation Function."""
    logger.info(
        "Starting Token Rotation Function",
        extra={
            "event": event,
            "context": context,
        }
    )

    try:
        wrapper = create_wrapper()
        wrapper.renew()
        wrapper.save()
    except TokenStillValid:
        logger.info("Token still valid, skipping renewal.")
    except Exception as e:
        logger.exception(str(e))
        raise
