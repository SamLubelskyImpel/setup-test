import logging

import requests

from .envs import LOG_LEVEL
from .s3 import save_token_to_s3
from .schemas import Token, TekionCredentials

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


class TekionTokenWrapper:
    """Wrapper class to handle Tekion Access Token."""

    def __init__(self, credentials: TekionCredentials, token: Token | None = None):
        self.credentials = credentials
        self.token = token

    def renew(self) -> Token:
        """Renews the access token."""
        if self.token and not self.token.expired:  # pragma: no cover
            logger.info("Renewing valid token")

        logger.debug("Requesting new token")

        resp = requests.post(
            url=self.credentials.auth_uri,
            headers=self.credentials.headers,
            data=self.credentials.data,
        )
        resp.raise_for_status()

        resp_data = resp.json()

        self.token = Token(
            token=resp_data["data"]["access_token"],
            token_type=resp_data["data"]['token_type'],
            expires_in_seconds=resp_data["data"]['expire_in'],
        )

        logger.info("Token renewed successfully")

        return self.token

    def save(self) -> None:
        """Saves the token to S3."""
        save_token_to_s3(token=self.token)
