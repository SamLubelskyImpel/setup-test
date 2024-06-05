import requests
from aws_lambda_powertools import Logger

from .exceptions import TokenStillValid
from .s3 import save_token_to_s3
from .schemas import Token, TekionCredentials

logger = Logger()


class TekionTokenWrapper:
    """Wrapper class to handle Tekion Access Token."""

    def __init__(self, credentials: TekionCredentials, token: Token | None = None):
        self.credentials = credentials
        self.token = token

    def renew(self) -> Token:
        """Renews the access token."""
        if self.token and not self.token.expired:
            raise TokenStillValid()

        logger.debug(
            "Requesting new token",
            extra={
                "credentials": self.credentials.model_dump_json()
            }
        )

        resp = requests.post(
            url=self.credentials.auth_uri,
            headers=self.credentials.headers,
            data=self.credentials.data,
        )
        resp.raise_for_status()

        resp_data = resp.json()

        self.token = Token(
            token=resp_data["access_token"],
            token_type=resp_data['token_type'],
            expires_in_seconds=resp_data['expires_in'],
        )

        logger.debug("Token received")

        return self.token

    def save(self) -> None:
        """Saves the token to S3."""
        save_token_to_s3(token=self.token)
