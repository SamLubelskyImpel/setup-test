from unittest.mock import patch

import pytest
import requests_mock

from access_token.token_wrapper import TekionTokenWrapper


@pytest.fixture
def wrapper(aws_s3, token, token_creds):
    return TekionTokenWrapper(credentials=token_creds, token=token)


def test_renew_when_token_is_expired(wrapper, token, token_creds):
    expected = {
        "access_token": "token-content",
        "token_type": "Bearer",
        "expires_in": 3600,
    }
    with requests_mock.Mocker() as m:
        m.register_uri(method='POST', url=token_creds.auth_uri, json={"data": expected})

        # force token as expired
        token.expires_in_seconds = -100
        assert token.expired is True

        result = wrapper.renew()
        assert result == wrapper.token

        assert result.token == expected["access_token"]
        assert result.token_type == expected["token_type"]
        assert result.expires_in_seconds == expected["expires_in"]

        assert result.expired is False


def test_renew_when_no_previous_token(wrapper, token, token_creds):
    expected = {
        "access_token": "token-content",
        "token_type": "Bearer",
        "expires_in": 3600,
    }
    with requests_mock.Mocker() as m:
        m.register_uri(method='POST', url=token_creds.auth_uri, json={"data": expected})

        # force token as none
        wrapper.token = None

        result = wrapper.renew()
        assert result == wrapper.token
        assert result.token == expected["access_token"]
        assert result.token_type == expected["token_type"]
        assert result.expires_in_seconds == expected["expires_in"]

        assert result.expired is False


@patch("access_token.token_wrapper.save_token_to_s3")
def test_save(mock_s3_saver, wrapper):
    wrapper.save()
    mock_s3_saver.assert_called_once_with(token=wrapper.token)
