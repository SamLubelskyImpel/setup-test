"""Configuration for pytest tests.

This conftest.py sets up environment variables *before* test collection,
which is necessary if modules being imported at collection time (like send_activity.py
instantiating CrmApiWrapper) depend on those environment variables.
"""
import os
import sys
import json
import pytest
from unittest.mock import patch, MagicMock
from moto import mock_aws
import boto3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from pathlib import Path

# Get the root directory (crm-integrations)
app_dir = os.path.join(os.path.dirname(__file__), '..', 'app')
sys.path.insert(0, app_dir)

module_name_lookup = {
    "test_send_activity": "send_activity",
    "test_monitoring": "monitoring",
    "test_invoke_data_pull": "invoke_data_pull",
    "test_lead_updates": "lead_updates",
    "test_dealerpeak_transform_data": "dealerpeak_transform_data"
}

# Environmental Variable Configuration: runs before modules are collected
def pytest_configure(config):
    """Allows plugins and conftest files to perform initial configuration."""
    os.environ["UPLOAD_SECRET_KEY"] = "impel"
    os.environ["LOGLEVEL"] = os.environ.get("LOGLEVEL", "DEBUG")
    os.environ["ENVIRONMENT"] = os.environ.get("ENVIRONMENT", "test")
    os.environ["SECRET_KEY"] = os.environ.get("SECRET_KEY", "DEALERPEAK")
    os.environ["CRM_API_DOMAIN"] = os.environ.get("CRM_API_DOMAIN", "https://test-crm.example.com/global")
    os.environ["REPORTING_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:TestReportingTopic"
    os.environ["INTEGRATIONS_BUCKET"] = "test-bucket"


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables."""

# External API Mocks
@pytest.fixture
def mock_crm_api(mocker, mock_salesperson_data, request):
    """Set up mock CRM API."""
    module_path = request.module.__name__
    mock_api = mocker.patch(f"{module_name_lookup[module_path]}.CrmApiWrapper")
    mock_instance = mock_api.return_value
    mock_instance.get_salesperson.return_value = mock_salesperson_data
    return mock_instance

@pytest.fixture
def mock_dealerpeak_api(mocker, request):
    """Set up mock Dealerpeak API."""
    module_path = request.module.__name__
    mock_api = mocker.patch(f"{module_name_lookup[module_path]}.DealerpeakApiWrapper")
    mock_instance = mock_api.return_value
    mock_instance.create_activity.return_value = "dp_task_id_123"
    return mock_api

# Test Data Mocks
@pytest.fixture(scope="module")
def mock_lead_data():
    """Mock lead data returned by DealerPeak API."""
    return {
        "agent": {
            "userID": "agent123",
            "givenName": "John",
            "familyName": "Doe",
            "contactInformation": {
                "phoneNumbers": [
                    {"type": "home", "number": "555-123-4522"},
                    {"type": "mobile", "number": "555-123-4567"},
                    {"type": "office", "number": "555-987-6543"}
                ],
                "emails": [
                    {"address": "john.doe@dealerpeak.test"},
                    {"address": "john.doe2@dealerpeak.test"}
                ]
            }
        },
        "status": {
            "status": "ACTIVE"
        }
    }

@pytest.fixture(scope="module")
def mock_salesperson_data():
    """Mock salesperson data used in tests."""
    return {
        "crm_salesperson_id": "agent123",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@dealerpeak.test",
        "phone": "555-123-4567",
        "position_name": "Agent",
        "is_primary": True
    }

@pytest.fixture(scope="module")
def mock_salesperson_data_list():
    """Mock parsed salesperson data in list format."""
    return {
        "crm_salesperson_id": "agent123",
        "first_name": "John",
        "last_name": "Doe",
        "email": ["john.doe@dealerpeak.test"],
        "phone": ["555-123-4567"],
        "position_name": "Agent",
        "is_primary": True
    }

@pytest.fixture(scope="module")
def mock_activity_data():
    """Mock activity data used in tests."""
    return {
        "lead_id": "test_lead_123",
        "activity_id": "test_activity_789",
        "dealer_integration_partner_id": "dealer_partner_abc",
        "activity_type": "Call"
    }

@pytest.fixture(scope="module")
def mock_secret_data():
    """Mock secret data structure returned by Secrets Manager."""
    return {
        "API_URL": "https://api.dealerpeak.test",
        "API_USERNAME": "test_user",
        "API_PASSWORD": "test_pass"
    }

@pytest.fixture(scope="module")
def mock_activity_data():
    """Mock activity data used in tests."""
    return {
        "lead_id": "test_lead_123",
        "activity_id": "test_activity_789",
        "dealer_integration_partner_id": "dealer_partner_abc",
        "activity_type": "Call"
    }

@pytest.fixture(scope="module")
def mock_event():
    """Mock event record returned by DealerPeak API."""
    return {
        "Records": [
            {
                "body": json.dumps({
                    'start_time': '2024-01-15T00:00:00Z',
                    'end_time': '2024-01-16T00:00:00Z',
                    'crm_dealer_id': 'dealer123__loc456',
                    'product_dealer_id': 'dealer123'
                })
            }
        ]
    }
# Moto Mocks

@pytest.fixture(scope="function")
def setup_secret(mock_secret_data):
    """Set up the secret in the current mock environment."""
    with mock_aws():
        secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
        secret_value = {
            "DEALERPEAK": json.dumps(mock_secret_data)
        }
        secrets_client.create_secret(
            Name="test/crm-integrations-partner",
            SecretString=json.dumps(secret_value)
        )
        yield secrets_client

@pytest.fixture(scope="function")
def setup_s3():
    """Set up the S3 bucket in the current mock environment."""
    with mock_aws():
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(
            Bucket='test-bucket'
        )
        yield s3_client

@pytest.fixture(scope="function")
def setup_sqs():
    """Set up the SQS queue in the current mock environment."""
    with mock_aws():
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        
        # Create the SQS queue
        queue_url = "https://sqs.test.amazonaws.com/123456789012/test-queue"
        sqs_client.create_queue(
            QueueName='test-queue'
        )
        yield sqs_client


@pytest.fixture(scope="module")
def mock_sqs_record_data(mock_activity_data):
    """Mock SQS record data used in tests."""
    return {
        "messageId": "msg1",
        "receiptHandle": "handle1",
        "body": json.dumps(mock_activity_data),
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1523232000000",
            "SenderId": "AIDAIENQZJ7EXAMPLE",
            "ApproximateFirstReceiveTimestamp": "1523232000001"
        },
        "messageAttributes": {},
        "md5OfBody": "0d8f3519518737163876476785853757",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
        "awsRegion": "us-east-1"
    }


@pytest.fixture
def sqs_record(mock_sqs_record_data):
    """Create an SQS record for testing."""
    return SQSRecord(mock_sqs_record_data)


