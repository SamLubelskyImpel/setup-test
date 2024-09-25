from unittest.mock import patch
from urllib.parse import urljoin

import pytest

from access_token.schemas import TekionCredentials


@pytest.fixture()
def credentials():
    return TekionCredentials(
        url="https://example.com",
        app_id="app_id",
        secret_key="secret_key",
    )


def test_credentials_headers(credentials):
    assert credentials.headers == {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def test_credentials_data(credentials):
    assert credentials.data == {
        "app_id": "app_id",
        "secret_key": "secret_key"
    }


TEST_AUTH_URI = "/fake-auth-uri"


@patch("access_token.schemas.CRM_TEKION_AUTH_ENDPOINT", TEST_AUTH_URI)
def test_auth_uri(credentials):
    assert credentials.auth_uri == urljoin(credentials.url, TEST_AUTH_URI)
