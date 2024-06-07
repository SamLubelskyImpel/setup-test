import os

import boto3
import pytest
from moto import mock_aws

from access_token.schemas import Token, TekionCredentials


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def aws_s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name=os.environ["AWS_DEFAULT_REGION"])


@pytest.fixture(scope="function")
def aws_secret_manager(aws_credentials):
    with mock_aws():
        yield boto3.client(
            "secretsmanager", region_name=os.environ["AWS_DEFAULT_REGION"]
        )


@pytest.fixture
def token() -> Token:
    return Token(
        token="fake-token",
        token_type="Bearer",
        expires_in_seconds=3600,
    )


@pytest.fixture
def token_creds() -> TekionCredentials:
    return TekionCredentials(
        url="http://fake-auth-uri.com",
        access_key="fake_access_key",
        secret_key="fake_secret_key",
        client_id="fake_client_id"
    )
