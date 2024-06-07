from unittest.mock import patch
from urllib.parse import urljoin

import pytest

from access_token.schemas import TekionCredentials


@pytest.fixture()
def credentials():
    return TekionCredentials(
        url="https://example.com",
        access_key="access_key",
        secret_key="secret_key",
        client_id="client_id",
    )


def test_credentials_headers(credentials):
    assert credentials.headers == {
        "accept": "application/json",
        "client_id": "client_id",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def test_credentials_data(credentials):
    assert credentials.data == {
        "access-key": "access_key",
        "secret-key": "secret_key"
    }


TEST_AUTH_URI = "/fake-auth-uri"


@patch("access_token.schemas.CRM_TEKION_AUTH_ENDPOINT", TEST_AUTH_URI)
def test_auth_uri(credentials):
    assert credentials.auth_uri == urljoin(credentials.url, TEST_AUTH_URI)
