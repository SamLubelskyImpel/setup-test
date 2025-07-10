"""Test suite for the DealerPeak data pull lambda.

This test suite covers:
1. Retrieval and parsing of DealerPeak API secrets.
2. Fetching and filtering of leads from DealerPeak API.
3. Saving raw leads to S3.
4. Error handling for API failures and invalid data.
5. End-to-end lambda handler execution flow.
"""

import json
import logging
from unittest.mock import MagicMock
import pytest
from moto import mock_aws
import boto3
from requests.exceptions import RequestException

from invoke_data_pull import (
    get_secrets,
    fetch_new_leads,
    filter_leads,
    save_raw_leads,
    record_handler,
    lambda_handler
)

MODULE_PATH = "invoke_data_pull"  # For patching module-level items

@pytest.fixture(scope="module")
def mock_initial_leads():
    """Mock initial leads list returned by DealerPeak API."""
    return [
        {
            "leadID": "lead123",
            "dateCreated": "January, 15 2024 10:30:00"
        },
        {
            "leadID": "lead456",
            "dateCreated": "January, 16 2024 11:00:00"
        },
        {
            "leadID": "lead789",
            "dateCreated": "January, 14 2024 10:30:00"  # After end time
        }
    ]

@pytest.fixture(scope="module")
def mock_invalid_leads():
    """Mock invalid lead record returned by DealerPeak API."""
    return [
        {
            "leadID": "lead1",
            "dateCreated": "January, 15 2024 10:30:00"
        },
        {
            "leadID": "lead2",
            "dateCreated": "Invalid Date Format"
        },
        {
            "leadID": "lead3",
            "dateCreated": "January, 17 2024 10:30:00"
        },
        {
            "leadID": "lead4",
            "dateCreated": "January, 14 2024 10:30:00"
        }
    ]

@pytest.fixture(scope="module")
def mock_lead_record():
    """Mock detailed lead record returned by DealerPeak API."""
    return {
        "leadID": "lead123",
        "dateCreated": "January, 15 2024 10:30:00",
        "customer": {
            "firstName": "John",
            "lastName": "Doe",
            "email": "john.doe@example.com"
        },
        "status": "NEW"
    }

@pytest.fixture(scope="module")
def mock_lead_record_2():
    """Mock detailed lead record returned by DealerPeak API."""
    return {
        "leadID": "lead456",
        "dateCreated": "January, 16 2024 11:00:00",
        "customer": {
            "firstName": "Jane",
            "lastName": "Doe",
            "email": "jane.doe@example.com"
        },
        "status": "NEW"
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


@pytest.fixture(scope="module")
def mock_partial_failure_event():
    """Mock event record with partial failure."""
    return {
        "Records": [
            {
                "messageId": "0",
                "body": json.dumps({
                    'start_time': '2024-01-15T00:00:00Z',
                    'end_time': '2024-01-16T00:00:00Z',
                    'crm_dealer_id': 'dealer123__loc456',
                    'product_dealer_id': 'dealer123'
                })
            },
            {
                "messageId": "1",
                "body": json.dumps({
                    'start_time': '2024-01-15T00:00:00Z',
                    'end_time': '2024-01-16T00:00:00Z',
                    'crm_dealer_id': 'dealer123__loc456',
                    'product_dealer_id': 'dealer123'
                })
            }
        ]
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
    print("changes covered by tests")
    with pytest.raises(Exception):
        get_secrets()

def test_fetch_new_leads(mocker, setup_secret, mock_initial_leads, mock_lead_record, mock_lead_record_2):
    """Test successful fetching of new leads from DealerPeak API."""
    mock_get = mocker.patch(f"{MODULE_PATH}.requests.get")
    
    # Mock initial leads list response
    mock_get.return_value.json.return_value = mock_initial_leads
    mock_get.return_value.status_code = 200
    
    # Mock detailed lead record response
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_initial_leads, status_code=200),
        MagicMock(json=lambda: mock_lead_record, status_code=200),
        MagicMock(json=lambda: mock_lead_record_2, status_code=200),
    ]
    
    start_time = "2024-01-15T00:00:00Z"
    end_time = "2024-01-16T00:00:00Z"
    crm_dealer_id = "dealer123__loc456"
    
    leads = fetch_new_leads(start_time, end_time, crm_dealer_id)
    
    assert len(leads) == 2
    
    # Verify API calls
    assert mock_get.call_count == 3
    mock_get.assert_any_call(
        url="https://api.dealerpeak.test/dealergroup/dealer123/location/loc456/leads",
        params={"deltaDate": start_time},
        auth=mocker.ANY,
        timeout=3
    )
    mock_get.assert_any_call(
        url="https://api.dealerpeak.test/dealergroup/dealer123/lead/lead123",
        auth=mocker.ANY,
        timeout=3
    )
    mock_get.assert_any_call(
        url="https://api.dealerpeak.test/dealergroup/dealer123/lead/lead456",
        auth=mocker.ANY,
        timeout=3
    )


def test_fetch_new_leads_initial_api_error(mocker, setup_secret):
    """Test error handling when initial lead fetching DealerPeak API call fails."""
    mock_get = mocker.patch(f"{MODULE_PATH}.requests.get")
    mock_get.side_effect = RequestException("API Error")
    
    with pytest.raises(RequestException, match="API Error"):
        fetch_new_leads("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z", "dealer123__loc456")

def test_fetch_new_leads_secondary_api_error(mocker, setup_secret, mock_initial_leads, mock_lead_record_2, caplog):
    """Test that if any of the individual lead retrieval api calls fail, the function logs that
    failure and continues to the next lead."""
    mock_get = mocker.patch(f"{MODULE_PATH}.requests.get")
    mock_get.side_effect = [
        MagicMock(json=lambda: mock_initial_leads, status_code=200),
        RequestException("API Error"),
        MagicMock(json=lambda: mock_lead_record_2, status_code=200),
    ]

    leads = fetch_new_leads("2024-01-15T00:00:00Z", "2024-01-16T00:00:00Z", "dealer123__loc456")
    
    assert len(leads) == 1
    assert leads[0]["leadID"] == "lead456"
    assert "Error fetching lead lead123. Skipping lead" in caplog.text

def test_filter_leads(mock_initial_leads):
    """Test filtering of leads by date."""
    
    start_time = "2024-01-15T00:00:00Z"
    filtered = filter_leads(mock_initial_leads, start_time)
    
    assert len(filtered) == 2
    assert filtered[0]["leadID"] == "lead123"


def test_filter_leads_invalid_date(mock_invalid_leads, caplog):
    """Test handling of invalid date formats in leads."""
    caplog.set_level(logging.ERROR)
    
    filtered = filter_leads(mock_invalid_leads, "2024-01-15T00:00:00Z")
    
    assert len(filtered) == 2
    assert "Error parsing dateCreated for lead lead2" in caplog.text


@mock_aws
def test_save_raw_leads(setup_s3, mock_initial_leads):
    """Test saving raw leads to S3."""

    save_raw_leads(mock_initial_leads, "dealer123")
    
    # Verify the file was saved to S3
    s3_client = boto3.client('s3', region_name='us-east-1')
    response = s3_client.list_objects_v2(
        Bucket='test-bucket',
        Prefix='raw/dealerpeak/dealer123/'
    )
    
    assert 'Contents' in response
    assert len(response['Contents']) == 1
    
    # Verify the content
    obj = s3_client.get_object(
        Bucket='test-bucket',
        Key=response['Contents'][0]['Key']
    )
    saved_leads = json.loads(obj['Body'].read().decode('utf-8'))
    assert saved_leads == mock_initial_leads


@mock_aws
def test_record_handler_success(mocker, setup_secret, setup_s3, mock_initial_leads, mock_event):
    """Test successful processing of a record."""
    mock_fetch = mocker.patch(f"{MODULE_PATH}.fetch_new_leads")
    mock_save = mocker.patch(f"{MODULE_PATH}.save_raw_leads")
    mock_fetch.return_value = mock_initial_leads
    
    record = mock_event["Records"][0]
    
    record_handler(record)
    
    mock_fetch.assert_called_once_with(
        '2024-01-15T00:00:00Z',
        '2024-01-16T00:00:00Z',
        'dealer123__loc456'
    )
    mock_save.assert_called_once_with(mock_initial_leads, 'dealer123')


@mock_aws
def test_record_handler_no_leads(mocker, setup_secret, setup_s3, mock_event, caplog):
    """Test handling of case when no new leads are found."""
    caplog.set_level(logging.INFO)
    
    mock_fetch = mocker.patch(f"{MODULE_PATH}.fetch_new_leads")
    mock_fetch.return_value = []
    
    record = mock_event["Records"][0]

    record_handler(record)
    
    assert "No new leads found for dealer dealer123" in caplog.text


@mock_aws
def test_record_handler_error(mocker, setup_secret, setup_s3, mock_event, caplog):
    """Test error handling in record handler."""
    caplog.set_level(logging.ERROR)
    
    mock_fetch = mocker.patch(f"{MODULE_PATH}.fetch_new_leads")
    mock_fetch.side_effect = Exception("Test error")
    
    record = mock_event["Records"][0]
    
    with pytest.raises(Exception, match="Test error"):
        record_handler(record)
    
    assert "[SUPPORT ALERT] Failed to Get Leads" in caplog.text


@mock_aws
def test_lambda_handler_success(mocker, mock_initial_leads, setup_secret, setup_s3, mock_event):
    """Test successful lambda handler execution."""
    mock_fetch = mocker.patch(f"{MODULE_PATH}.fetch_new_leads")
    mock_fetch.return_value = mock_initial_leads
    
    response = lambda_handler(mock_event, MagicMock())
    
    assert response["batchItemFailures"] == []


@mock_aws
def test_lambda_handler_error(mocker, mock_event, setup_secret, setup_s3):
    """Test lambda handler error handling."""
    mock_fetch = mocker.patch(f"{MODULE_PATH}.fetch_new_leads")
    mock_fetch.side_effect = Exception("Test error")
    
    with pytest.raises(Exception) as exc_info:
        lambda_handler(mock_event, MagicMock())
    
    assert "All records failed processing" in str(exc_info.value)
    assert "Test error" in str(exc_info.value)

@mock_aws
def test_lambda_handler_partial_success(mocker, mock_initial_leads, setup_secret, setup_s3,
                                        mock_partial_failure_event, caplog):
    """Test lambda handler partial success handling."""
    mock_record_handler = mocker.patch(f"{MODULE_PATH}.record_handler")
    mock_record_handler.side_effect = [
        Exception("Test error"),
        mock_initial_leads
    ]
    
    response = lambda_handler(mock_partial_failure_event, MagicMock())
    
    assert response["batchItemFailures"] == [
        {
            "itemIdentifier": "0"
        }
    ]
    assert mock_record_handler.call_count == 2  # Both records were processed

def test_fail():
    assert False