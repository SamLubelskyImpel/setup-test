"""Configure pytest for the Activix integration tests."""

import sys
from pathlib import Path
from os import environ
import pytest
from moto import mock_aws
import boto3

# Get the absolute path to the app directory
app_path = str(Path(__file__).parent.parent / 'app')

# Add the app directory to Python path
sys.path.insert(0, app_path)

environ["ENVIRONMENT"] = "test"
environ["CRM_API_DOMAIN"] = "api.test.crm.com"
environ["UPLOAD_SECRET_KEY"] = "impel"


@pytest.fixture
def s3_client():
    """Create a mocked S3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def secretsmanager_client():
    """Create a mocked Secrets Manager client."""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="us-east-1")
