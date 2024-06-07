import json
from unittest.mock import patch

import pytest

from access_token.secrets import get_credentials_from_secrets

TEST_CRM_INTEGRATION_SECRETS_ID = "test/tekion/fake-secret"
TEST_PARTNER_KEY = "fake_key"


@pytest.fixture
def secret_manager(aws_secret_manager, token_creds):
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    sample_secret = {
        TEST_PARTNER_KEY: token_creds.as_dict()
    }
    aws_secret_manager.create_secret(
        Name=TEST_CRM_INTEGRATION_SECRETS_ID,
        SecretString=json.dumps(sample_secret),
    )
    return aws_secret_manager


@patch(
    "access_token.secrets.CRM_INTEGRATION_SECRETS_ID", TEST_CRM_INTEGRATION_SECRETS_ID
)
@patch("access_token.secrets.PARTNER_KEY", TEST_PARTNER_KEY)
def test_fetching_credentials_from_secrets(secret_manager, token_creds):
    result = get_credentials_from_secrets()

    assert result.auth_uri == token_creds.auth_uri
    assert result.access_key == token_creds.access_key
    assert result.secret_key == token_creds.secret_key
    assert result.client_id == token_creds.client_id
