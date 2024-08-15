from unittest.mock import patch

from access_token.handler import create_wrapper
from access_token.token_wrapper import TekionTokenWrapper


@patch("access_token.handler.get_token_from_s3")
@patch("access_token.handler.get_credentials_from_secrets")
def test_wrapper_creation(secrets_getter, last_token_getter, token_creds, token):
    secrets_getter.return_value = token_creds
    last_token_getter.return_value = token
    wrapper = create_wrapper()

    assert isinstance(wrapper, TekionTokenWrapper)
    assert wrapper.credentials == token_creds
    assert wrapper.token == token
