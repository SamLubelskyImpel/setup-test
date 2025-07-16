"""Test suite for the DealerPeak lead updates lambda.

This test suite covers:
1.  Retrieval and parsing of DealerPeak API secrets.
2.  Lead data retrieval and parsing from DealerPeak API.
3.  Salesperson data parsing and formatting.
4.  SQS message sending for lead updates.
5.  Error handling for API failures and invalid data.
6.  End-to-end lambda handler execution flow.
"""

import json
import logging
from unittest.mock import MagicMock, patch
import pytest
from moto import mock_aws
from requests.exceptions import RequestException
import boto3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from lead_updates import (
    get_secrets,
    get_lead,
    parse_salesperson,
    send_sqs_message,
    get_salesperson_by_lead_id,
    lambda_handler
)

MODULE_PATH = "lead_updates"  # For patching module-level items


@pytest.fixture(scope="function")
def setup_sqs_and_s3():
    """Set up the S3 bucket and configuration file in the current mock environment."""
    with mock_aws():
        s3_client = boto3.client('s3', region_name='us-east-1')
        sqs_client = boto3.client('sqs', region_name='us-east-1')
        
        # Create the bucket
        s3_client.create_bucket(
            Bucket='test-bucket'
        )
        
        # Create the SQS queue
        queue_url = "https://sqs.test.amazonaws.com/123456789012/test-queue"
        sqs_client.create_queue(
            QueueName='test-queue'
        )
        
        # Upload the configuration file
        config_data = {
            "lead_updates_queue_url": queue_url
        }
        s3_client.put_object(
            Bucket='test-bucket',
            Key='configurations/test_DEALERPEAK.json',
            Body=json.dumps(config_data).encode('utf-8')
        )
        yield s3_client


@pytest.fixture
def mock_message_body():
    """Fixture providing a sample message body for SQS tests."""
    return {
        "lead_id": "lead123",
        "dealer_integration_partner_id": "partner456",
        "status": "ACTIVE",
        "salespersons": [{"id": "agent789"}]
    }


@mock_aws
def test_get_secrets(setup_secret, mock_secret_data):
    """Test successful retrieval of DealerPeak API secrets."""
    api_url, username, password = get_secrets()
    
    assert api_url == mock_secret_data["API_URL"]
    assert username == mock_secret_data["API_USERNAME"]
    assert password == mock_secret_data["API_PASSWORD"]

@mock_aws
def test_get_secrets_not_found():
    """Test error handling when secret is not found."""
    with pytest.raises(Exception):
        get_secrets()


def test_get_lead(mocker, setup_secret, mock_lead_data):
    """Test successful lead retrieval from DealerPeak API."""
    mock_get = mocker.patch(f"{MODULE_PATH}.get")
    mock_get.return_value.json.return_value = mock_lead_data
    mock_get.return_value.status_code = 200
    
    result = get_lead("dealer123__loc456", "lead789")
    
    assert result == mock_lead_data
    mock_get.assert_called_once_with(
        url="https://api.dealerpeak.test/dealergroup/dealer123/lead/lead789",
        auth=mocker.ANY,
        timeout=3
    )

def test_parse_salesperson(mock_lead_data, mock_salesperson_data):
    """Test successful parsing of salesperson data."""
    result = parse_salesperson(mock_lead_data["agent"])
    assert result == mock_salesperson_data


def test_parse_salesperson_list_format(mock_lead_data, mock_salesperson_data_list):
    """Test successful parsing of salesperson data in list format."""
    result = parse_salesperson(mock_lead_data["agent"], format_list=True)
    assert result == mock_salesperson_data_list


def test_parse_salesperson_missing_fields():
    """Test parsing of salesperson data with missing fields."""
    minimal_agent = {
        "userID": "agent123",
        "contactInformation": {
            "phoneNumbers": [],
            "emails": []
        }
    }
    
    expected = {
        "crm_salesperson_id": "agent123",
        "first_name": "",
        "last_name": "",
        "email": "",
        "phone": "",
        "position_name": "Agent",
        "is_primary": True
    }
    
    result = parse_salesperson(minimal_agent)
    assert result == expected


@mock_aws
def test_send_sqs_message(setup_sqs_and_s3, mock_message_body):
    """Test successful sending of SQS message."""
    send_sqs_message(mock_message_body)
    
    # Verify the message was sent to the correct queue
    queue_url = "https://sqs.test.amazonaws.com/123456789012/test-queue"
    
    # Get messages from the queue
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1
    )
    
    assert 'Messages' in response
    message = json.loads(response['Messages'][0]['Body'])
    assert message == mock_message_body


@mock_aws
def test_send_sqs_message_error(caplog):
    """Test error handling when sending SQS message fails."""
    caplog.set_level(logging.ERROR)
    
    # Don't set up S3, so the get_object call will fail
    send_sqs_message({"test": "data"})
    
    assert "Error sending SQS message" in caplog.text


def test_get_salesperson_by_lead_id_success(
    mocker, mock_lead_data, mock_salesperson_data, setup_s3, caplog
):
    """Test successful retrieval of salesperson by lead ID."""
    mock_get_lead = mocker.patch(f"{MODULE_PATH}.get_lead", return_value=mock_lead_data)
    mock_send_sqs_message = mocker.patch(f"{MODULE_PATH}.send_sqs_message")

    status_code, body = get_salesperson_by_lead_id(
        crm_dealer_id="dealer123__loc456",
        crm_lead_id="lead789",
        lead_id="lead123",
        dealer_partner_id="partner456"
    )
    
    assert status_code == 200
    assert body["status"] == "ACTIVE"
    assert body["salespersons"] == [mock_salesperson_data]
    assert "Found lead" in caplog.text
    
    expected_message_body = {
        "lead_id": "lead123",
        "dealer_integration_partner_id": "partner456",
        "status": "ACTIVE",
        "salespersons": [mock_salesperson_data]
    }

    mock_send_sqs_message.assert_called_once_with(expected_message_body)
    mock_get_lead.assert_called_once_with("dealer123__loc456", "lead789")


def test_get_salesperson_by_lead_id_not_found(mocker, caplog):
    """Test handling of lead not found scenario."""
    mock_get_lead = mocker.patch(f"{MODULE_PATH}.get_lead", return_value=None)
    
    status_code, body = get_salesperson_by_lead_id(
        crm_dealer_id="dealer123__loc456",
        crm_lead_id="lead789",
        lead_id="lead123",
        dealer_partner_id="partner456"
    )
    
    assert status_code == 404
    assert "Lead not found" in caplog.text
    assert "Lead not found" in body["error"]


def test_get_salesperson_by_lead_id_api_error(mocker, caplog):
    """Test error handling when DealerPeak API call fails."""
    caplog.set_level(logging.ERROR)
    mock_get_lead = mocker.patch(
        f"{MODULE_PATH}.get_lead",
        side_effect=RequestException("API Error")
    )
    
    with pytest.raises(RequestException, match="API Error"):
        get_salesperson_by_lead_id(
            crm_dealer_id="dealer123__loc456",
            crm_lead_id="lead789",
            lead_id="lead123",
            dealer_partner_id="partner456"
        )
    
    assert "[SUPPORT ALERT] Failed to Get Lead Update" in caplog.text


@mock_aws
def test_lambda_handler_get_lead(mocker, mock_lead_data, mock_salesperson_data, setup_secret, setup_s3):
    """Test lambda handler for getting lead updates."""
    mock_get_lead = mocker.patch(f"{MODULE_PATH}.get_lead", return_value=mock_lead_data)
    
    event = {
        "crm_dealer_id": "dealer123__loc456",
        "dealer_integration_partner_id": "partner456",
        "lead_id": "lead123",
        "crm_lead_id": "lead789"
    }
    
    response = lambda_handler(event, MagicMock())
    
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "ACTIVE"
    assert body["salespersons"] == [mock_salesperson_data]


@mock_aws
def test_lambda_handler_get_employees(mocker, setup_secret, setup_s3):
    """Test lambda handler for getting employee list."""
    mock_get = mocker.patch(f"{MODULE_PATH}.get")
    mock_get.return_value.json.return_value = [
        {
            "userID": "agent123",
            "givenName": "John",
            "familyName": "Doe",
            "contactInformation": {
                "phoneNumbers": [{"type": "mobile", "number": "555-123-4567"}],
                "emails": [{"address": "john.doe@dealerpeak.test"}]
            }
        },
        {
            "userID": "agent456",
            "givenName": "Jane",
            "familyName": "Doe",
            "contactInformation": {
                "phoneNumbers": [{"type": "mobile", "number": "555-123-4568"}],
                "emails": [{"address": "jane.doe@dealerpeak.test"}]
            }
        },
    ]
    mock_get.return_value.status_code = 200
    
    event = {
        "crm_dealer_id": "dealer123__loc456"
    }
    
    response = lambda_handler(event, MagicMock())
    
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert len(body) == 2
    assert body[0]["crm_salesperson_id"] == "agent123"
    assert body[0]["email"] == ["john.doe@dealerpeak.test"]
    assert body[0]["phone"] == ["555-123-4567"] 