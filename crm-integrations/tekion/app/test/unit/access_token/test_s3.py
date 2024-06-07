from unittest.mock import patch

import pytest

from access_token.s3 import get_token_from_s3, save_token_to_s3

TEST_BUCKET = "fake-bucket"
TEST_AUTH_FILE = "auth/token.json"


@pytest.fixture
def s3_bucket(aws_s3):
    aws_s3.create_bucket(Bucket=TEST_BUCKET)
    return aws_s3


@patch("access_token.s3.INTEGRATIONS_BUCKET", TEST_BUCKET)
@patch("access_token.s3.TOKEN_FILE", TEST_AUTH_FILE)
def test_fetching_token_from_s3(s3_bucket, token):
    s3_bucket.put_object(
        Bucket=TEST_BUCKET,
        Key=TEST_AUTH_FILE,
        Body=token.as_json()
    )

    result = get_token_from_s3()

    assert result.token == token.token
    assert result.token_type == token.token_type
    assert result.expires_in_seconds == token.expires_in_seconds


@patch("access_token.s3.INTEGRATIONS_BUCKET", TEST_BUCKET)
@patch("access_token.s3.TOKEN_FILE", TEST_AUTH_FILE)
def test_save_token_to_s3(s3_bucket, token):
    save_token_to_s3(token=token)

    result = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=TEST_AUTH_FILE)

    assert result["Body"].read() == token.as_json().encode()


