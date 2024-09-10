from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from access_token.schemas import Token


@pytest.mark.parametrize(
    "expires_in_seconds, expired",
    [
        (10, False),
        (1440, False),
        (-10, True),
    ],
    ids=["valid - 10s ahead", "valid - 1440s ahead", "expired"],
)
def test_token_expiration(expires_in_seconds, expired):
    now = datetime.now()

    with patch("access_token.schemas.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        token = Token(
            token="test_token",
            expires_in_seconds=expires_in_seconds,
        )

        assert token.expires_at > now + timedelta(seconds=expires_in_seconds)
        assert token.expired is expired
