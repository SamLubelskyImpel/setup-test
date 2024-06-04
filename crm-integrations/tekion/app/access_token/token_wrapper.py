import logging
from os import environ

import requests

from .exceptions import TokenStillValid
from .s3 import save_token_to_s3
from .schemas import Token, TekionCredentials

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class TekionTokenWrapper:
    """Wrapper class to handle Tekion Access Token."""

    def __init__(self, credentials: TekionCredentials, token: Token | None = None):
        self.credentials = credentials
        self.token = token

    def renew(self) -> Token:
        """Renews the access token."""
        if self.token and not self.token.expired:
            raise TokenStillValid()

        resp = requests.post(
            url=self.credentials.auth_uri,
            headers=self.credentials.headers,
            data=self.credentials.data,
            verify=False
        )
        resp.raise_for_status()

        resp_data = resp.json()

        self.token = Token(
            token=resp_data["access_token"],
            token_type=resp_data['token_type'],
            expires_in_seconds=resp_data['expires_in'],
        )
        return self.token

    def save(self) -> None:
        """Saves the token to S3."""
        save_token_to_s3(token=self.token)
