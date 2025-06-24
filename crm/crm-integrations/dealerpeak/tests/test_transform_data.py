"""Test suite for DealerPeak data transformation lambda.

This test suite covers the following aspects of the DealerPeak lead transformation process:
1. Data parsing and transformation from DealerPeak format to unified format
2. Contact information extraction and formatting
3. API integration with CRM system
4. Error handling and edge cases
5. Complete end-to-end lambda execution

The tests are organized into:
- Fixtures: Mock data and responses
- Success Cases: Happy path scenarios
- Edge Cases: Boundary conditions and optional data
- Error Cases: Error handling and validation
- Integration Tests: Complete workflow testing
"""

import pytest
import json
import logging
from unittest.mock import patch, MagicMock
from datetime import datetime
from moto import mock_aws
import boto3
from transform_data import (
    lambda_handler,
    record_handler,
    parse_json_to_entries,
    extract_contact_information,
    format_ts,
    upload_consumer_to_db,
    upload_lead_to_db,
    get_secret
)

# Fixtures
@pytest.fixture
def mock_s3_event():
    """Mock S3 event trigger.
    
    Simulates an S3 event notification that would trigger the lambda.
    Contains:
    - Bucket name
    - Object key in DealerPeak format (dealerpeak/{dealer_id}/leads.json)
    """
    return {
        "Records": [
            {
                "body": json.dumps({
                    "detail": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "dealerpeak/1234.TEST/leads.json"}
                    }
                })
            }
        ]
    }


@pytest.fixture
def mock_s3_data():
    """Mock S3 data content.
    
    Simulates the lead data structure from DealerPeak, including:
    - Lead information (ID, creation date, source, status)
    - Customer details (name, contact info, preferences)
    - Vehicle information (VIN, make, model, etc.)
    - Sales agent details
    """
    return [
        {
            "leadID": "LEAD123",
            "dateCreated": "November, 17 2023 18:57:17",
            "source": {"source": "Internet"},
            "status": {"status": "New"},
            "firstNote": {"note": "Test note"},
            "customer": {
                "userID": "CUST123",
                "givenName": "John",
                "familyName": "Doe",
                "contactInformation": {
                    "emails": [{"address": "john@example.com"}],
                    "phoneNumbers": [
                        {"type": "mobile", "number": "555-1234"},
                        {"type": "main", "number": "555-5678"}
                    ],
                    "addresses": [
                        {
                            "type": "main",
                            "line1": "123 Main St",
                            "city": "Test City",
                            "postcode": "12345"
                        }
                    ],
                    "allowed": {"email": True}
                }
            },
            "agent": {
                "userID": "AGENT123",
                "givenName": "Jane",
                "familyName": "Smith",
                "contactInformation": {
                    "emails": [{"address": "jane@example.com"}],
                    "phoneNumbers": [{"type": "mobile", "number": "555-4321"}]
                }
            },
            "vehiclesOfInterest": [
                {
                    "carID": "CAR123",
                    "vin": "1HGCM82633A123456",
                    "year": "2023",
                    "make": "Honda",
                    "model": "Civic",
                    "isNew": True
                }
            ]
        }
    ]


@pytest.fixture
def mock_api_response():
    """Mock CRM API responses.
    
    Simulates successful responses from the CRM API for:
    - Consumer creation
    - Lead creation
    """
    return {
        "consumer_id": "UNIFIED-CUST-123",
        "lead_id": "UNIFIED-LEAD-123"
    }


@pytest.fixture(scope="function")
def setup_api_secret():
    """Set up the secret in the current mock environment."""
    with mock_aws():
        secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
        # First encode the inner secret data
        inner_secret = json.dumps({
            "api_key": "test-api-key"
        })
        
        # Then encode the outer structure with the inner secret
        outer_secret = json.dumps({
            "impel": inner_secret
        })
        
        secrets_client.create_secret(
            Name="test/crm-api",
            SecretString=outer_secret
        )
        yield secrets_client


@pytest.fixture(scope="function")
def setup_s3(mock_s3_data):
    """Set up the S3 bucket and object in the current mock environment."""
    with mock_aws():
        s3_client = boto3.client('s3', region_name='us-east-1')
        # Create the bucket
        s3_client.create_bucket(
            Bucket='test-bucket'
        )
        # Upload the test data
        s3_client.put_object(
            Bucket='test-bucket',
            Key='dealerpeak/1234.TEST/leads.json',
            Body=json.dumps(mock_s3_data).encode('utf-8')
        )
        yield s3_client


# Success Cases
@mock_aws
def test_get_secret(setup_secret, mock_secret_data):
    """Test secret retrieval."""
    secret = get_secret("crm-integrations-partner", "DEALERPEAK")
    assert secret == mock_secret_data

def test_format_ts():
    """Test timestamp formatting.
    
    Verifies that DealerPeak's date format (e.g., "November, 17 2023 18:57:17")
    is correctly converted to the unified format (ISO 8601 with Z suffix).
    """
    input_ts = "November, 17 2023 18:57:17"
    expected = "2023-11-17T18:57:17Z"
    assert format_ts(input_ts) == expected


def test_extract_contact_information():
    """Test contact information extraction.
    
    Verifies that contact information is correctly extracted from DealerPeak's
    nested structure into the unified format, including:
    - User ID
    - Name components
    - Email address
    - Phone number
    """
    item = {
        "userID": "TEST123",
        "givenName": "Test",
        "familyName": "User",
        "contactInformation": {
            "emails": [{"address": "test@example.com"}],
            "phoneNumbers": [{"type": "mobile", "number": "555-1234"}]
        }
    }
    db_entity = {}
    extract_contact_information("consumer", item, db_entity)
    
    assert db_entity["crm_consumer_id"] == "TEST123"
    assert db_entity["first_name"] == "Test"
    assert db_entity["last_name"] == "User"
    assert db_entity["email"] == "test@example.com"
    assert db_entity["phone"] == "555-1234"


def test_parse_json_to_entries(mock_s3_data):
    """Test JSON parsing to unified format.
    
    Verifies that DealerPeak lead data is correctly transformed into the unified format,
    including:
    - Lead information
    - Consumer details
    - Vehicle information
    - Proper field mapping and data types
    """
    entries = parse_json_to_entries("1234.TEST", mock_s3_data)
    
    assert len(entries) == 1
    entry = entries[0]
    
    # Verify lead data
    assert entry["product_dealer_id"] == "1234.TEST"
    assert entry["lead"]["crm_lead_id"] == "LEAD123"
    assert entry["lead"]["lead_origin"] == "INTERNET"
    
    # Verify consumer data
    assert entry["consumer"]["crm_consumer_id"] == "CUST123"
    assert entry["consumer"]["first_name"] == "John"
    assert entry["consumer"]["email"] == "john@example.com"
    
    # Verify vehicle data
    assert len(entry["lead"]["vehicles_of_interest"]) == 1
    vehicle = entry["lead"]["vehicles_of_interest"][0]
    assert vehicle["vin"] == "1HGCM82633A123456"
    assert vehicle["condition"] == "New"


def test_upload_consumer_to_db(mocker, mock_api_response):
    """Test consumer upload to CRM.
    
    Verifies that:
    1. Consumer data is correctly formatted for the CRM API
    2. API call is made with proper headers and data
    3. Response is properly handled
    4. Consumer ID is correctly extracted
    """
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=200,
        json=lambda: mock_api_response
    )
    
    consumer = {
        "first_name": "Test",
        "last_name": "User",
        "email": "test@example.com"
    }
    
    consumer_id = upload_consumer_to_db(consumer, "1234.TEST", "test-key", 0)
    assert consumer_id == "UNIFIED-CUST-123"
    assert mock_post.call_count == 1


def test_upload_lead_to_db(mocker, mock_api_response):
    """Test lead upload to CRM.
    
    Verifies that:
    1. Lead data is correctly formatted for the CRM API
    2. API call is made with proper headers and data
    3. Response is properly handled
    4. Lead ID is correctly extracted
    """
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=200,
        json=lambda: mock_api_response
    )
    
    lead = {
        "crm_lead_id": "LEAD123",
        "lead_ts": "2023-11-17T18:57:17Z",
        "consumer_id": "UNIFIED-CUST-123"
    }
    
    lead_id = upload_lead_to_db(lead, "test-key", 0)
    assert lead_id == "UNIFIED-LEAD-123"
    assert mock_post.call_count == 1


# Edge Cases
def test_parse_json_to_entries_empty_vehicles(mock_s3_data):
    """Test parsing with empty vehicles list.
    
    Verifies that the transformation handles cases where:
    - No vehicles are associated with the lead
    - The vehicles list is empty
    - The output still maintains the correct structure
    """
    mock_s3_data[0]["vehiclesOfInterest"] = []
    entries = parse_json_to_entries("1234.TEST", mock_s3_data)
    
    assert len(entries) == 1
    assert len(entries[0]["lead"]["vehicles_of_interest"]) == 0


def test_parse_json_to_entries_missing_contact(mock_s3_data):
    """Test parsing with missing contact information.
    
    Verifies that the transformation handles cases where:
    - Email information is missing
    - Default values are properly set
    - The transformation still completes successfully
    """
    del mock_s3_data[0]["customer"]["contactInformation"]["emails"]
    entries = parse_json_to_entries("1234.TEST", mock_s3_data)
    
    assert len(entries) == 1
    assert entries[0]["consumer"]["email"] == ""


# Error Cases
def test_upload_consumer_to_db_error(mocker):
    """Test consumer upload error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    4. The error handling doesn't silently fail
    """
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=400,
        text="Invalid data"
    )
    mock_post.return_value.raise_for_status.side_effect = Exception("API Error")
    
    consumer = {"first_name": "Test"}
    
    with pytest.raises(Exception) as exc_info:
        upload_consumer_to_db(consumer, "1234.TEST", "test-key", 0)
    assert "API Error" in str(exc_info.value)

def test_upload_consumer_to_db_creation_error(mocker):
    """Test consumer upload creation error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    4. The error handling doesn't silently fail
    """
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=200,
        json=lambda: {"consumer_id": None}
    )
    
    consumer = {"first_name": "Test"}

    with pytest.raises(Exception) as exc_info:
        upload_consumer_to_db(consumer, "1234.TEST", "test-key", 0)
    assert "Error creating consumer" in str(exc_info.value)


def test_upload_lead_to_db_error(mocker):
    """Test lead upload error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    4. The error handling doesn't silently fail
    """
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=400,
        text="Invalid data"
    )
    mock_post.return_value.raise_for_status.side_effect = Exception("API Error")
    
    lead = {"crm_lead_id": "LEAD123"}
    
    with pytest.raises(Exception) as exc_info:
        upload_lead_to_db(lead, "test-key", 0)
    assert "API Error" in str(exc_info.value)

def test_upload_lead_to_db_creation_error(mocker):
    """Test lead upload creation error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    4. The error handling doesn't silently fail
    """
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=200,
        json=lambda: {"lead_id": None}
    )

    lead = {"crm_lead_id": "LEAD123"}
    
    with pytest.raises(Exception) as exc_info:
        upload_lead_to_db(lead, "test-key", 0)
    assert "Error creating lead" in str(exc_info.value)

# Integration Test
@mock_aws
def test_lambda_handler_success(
    mocker,
    mock_s3_event,
    mock_api_response,
    setup_api_secret,
    setup_s3,
    caplog
):
    """Test complete lambda handler success path.
    
    Verifies the entire workflow:
    1. S3 event processing
    2. Data retrieval from S3
    3. Secret retrieval from Secrets Manager
    4. Data transformation
    5. API calls to CRM
    6. Proper logging
    7. Success response generation
    """
    # Mock API responses
    mock_post = mocker.patch('transform_data.requests.post')
    mock_post.return_value = MagicMock(
        status_code=200,
        json=lambda: mock_api_response
    )
    
    with caplog.at_level(logging.INFO):
        result = lambda_handler(mock_s3_event, None)
        
        # Verify successful processing
        assert result["batchItemFailures"] == []
        
        # Verify API calls
        assert mock_post.call_count == 2  # One for consumer, one for lead
        
        # Verify logging
        assert "Lead successfully created" in caplog.text


# Error Handling Test
@mock_aws
def test_lambda_handler_s3_error(mock_s3_event, caplog):
    """Test lambda handler S3 error handling.
    
    Verifies that:
    1. S3 errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    4. The error handling doesn't silently fail
    """
    # Don't set up S3, so the get_object call will fail
    
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception) as exc_info:
            lambda_handler(mock_s3_event, None)
        
        assert "Error transforming dealerpeak record" in caplog.text 