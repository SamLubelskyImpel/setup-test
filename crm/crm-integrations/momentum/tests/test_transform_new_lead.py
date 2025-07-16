"""Test suite for Momentum data transformation lambda.

This test suite covers the following aspects of the Momentum lead transformation process:
1. Data parsing and transformation from Momentum format to unified format
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
from transform_new_lead import (
    lambda_handler,
    record_handler,
    parse_lead,
    parse_phone_number,
    parse_address,
    extract_salesperson,
    get_secret,
    get_existing_lead,
    get_existing_consumer,
    get_recent_leads,
    create_consumer,
    create_lead
)


# Fixtures
@pytest.fixture
def mock_sqs_event():
    """Mock SQS event trigger.
    
    Simulates an SQS event notification that would trigger the lambda.
    Contains:
    - Bucket name
    - Object key in Momentum format
    - Product dealer ID
    """
    return {
        "Records": [
            {
                "body": json.dumps({
                    "bucket": "test-bucket",
                    "key": "momentum/1234.TEST/leads.json",
                    "product_dealer_id": "TEST123"
                })
            }
        ]
    }


@pytest.fixture
def mock_s3_data():
    """Mock S3 data content.
    
    Simulates the lead data structure from Momentum, including:
    - Lead information (ID, creation date, source, status)
    - Customer details (name, contact info, preferences)
    - Vehicle information (VIN, make, model, etc.)
    - Sales agent details
    """
    return {
        "id": "LEAD123",
        "dealerID": "DEALER456",
        "firstName": "John",
        "lastName": "Doe",
        "personApiID": "CONSUMER789",
        "cellPhone": "123-456-7890",
        "email": "john.doe@example.com",
        "address1": "123 Main St",
        "city": "Anytown",
        "country": "USA",
        "zip": "12345",
        "date": "2024-01-26T17:16:19-0800",
        "leadStatus": "New",
        "leadType": "Internet",
        "providerName": "Momentum",
        "comments": "Test lead",
        "vehicleType": "New",
        "vin": "1HGCM82633A123456",
        "stock": "STOCK123",
        "make": "Honda",
        "model": "Accord",
        "year": "2023",
        "color": "Blue",
        "trim": "EX-L",
        "bdcID": "BDC123",
        "bdcName": "Jane Smith"
    }


@pytest.fixture
def mock_secret():
    """Mock Secrets Manager response.
    
    Simulates the nested JSON structure of the CRM API secret:
    1. Inner secret contains the API key
    2. Outer structure contains the inner secret under the UPLOAD_SECRET_KEY
    """
    inner_secret = json.dumps({
        "api_key": "test-api-key"
    })
    
    outer_secret = json.dumps({
        "impel": inner_secret
    })
    
    return {
        "SecretString": outer_secret
    }


@pytest.fixture
def mock_api_responses():
    """Mock API responses for CRM endpoints."""
    return {
        "get_existing_lead": MagicMock(
            status_code=404,
            json=lambda: {"lead_id": None}
        ),
        "get_existing_consumer": MagicMock(
            status_code=404,
            json=lambda: {"consumer_id": None}
        ),
        "get_recent_leads": MagicMock(
            status_code=200,
            json=lambda: {"leads": []}
        ),
        "create_consumer": MagicMock(
            status_code=200,
            json=lambda: {"consumer_id": "NEW_CONSUMER123"}
        ),
        "create_lead": MagicMock(
            status_code=200,
            json=lambda: {"lead_id": "NEW_LEAD123"}
        )
    }


# Success Cases
def test_parse_phone_number():
    """Test phone number parsing.
    
    Verifies that phone numbers are correctly extracted and formatted
    from the Momentum data structure.
    """
    data = {
        "cellPhone": "123-456-7890",
        "homePhone": "",
        "workPhone": ""
    }
    assert parse_phone_number(data) == "1234567890"

    data = {
        "cellPhone": "",
        "homePhone": "(555) 123-4567",
        "workPhone": ""
    }
    assert parse_phone_number(data) == "5551234567"

    data = {
        "cellPhone": "",
        "homePhone": "",
        "workPhone": "555.123.4567"
    }
    assert parse_phone_number(data) == "5551234567"


def test_parse_address():
    """Test address parsing.
    
    Verifies that addresses are correctly combined and formatted
    from the Momentum data structure.
    """
    data = {
        "address1": "123 Main St",
        "address2": "Apt 4B"
    }
    assert parse_address(data) == "123 Main St Apt 4B"

    data = {
        "address1": "123 Main St",
        "address2": None
    }
    assert parse_address(data) == "123 Main St"

    data = {
        "address1": None,
        "address2": "Apt 4B"
    }
    assert parse_address(data) == "Apt 4B"


def test_extract_salesperson():
    """Test salesperson data extraction.
    
    Verifies that salesperson information is correctly extracted
    and formatted from the Momentum data structure.
    """
    # Test with full name
    result = extract_salesperson("SP123", "John Smith", "BDC Rep")
    assert result == {
        "crm_salesperson_id": "SP123",
        "first_name": "John",
        "last_name": "Smith",
        "is_primary": True,
        "position_name": "BDC Rep"
    }

    # Test with single name
    result = extract_salesperson("SP123", "John", "BDC Rep")
    assert result == {
        "crm_salesperson_id": "SP123",
        "first_name": "John",
        "last_name": "",
        "is_primary": True,
        "position_name": "BDC Rep"
    }

    # Test with no name
    result = extract_salesperson("SP123")
    assert result == {
        "crm_salesperson_id": "SP123",
        "first_name": "",
        "last_name": "",
        "is_primary": True,
        "position_name": "Primary Salesperson"
    }


def test_parse_lead():
    """Test lead data parsing.
    
    Verifies that the complete lead data structure is correctly
    parsed and transformed from Momentum format to unified format.
    """
    product_dealer_id = "TEST123"
    data = {
        "id": "LEAD123",
        "dealerID": "DEALER456",
        "firstName": "John",
        "lastName": "Doe",
        "personApiID": "CONSUMER789",
        "cellPhone": "123-456-7890",
        "email": "john.doe@example.com",
        "address1": "123 Main St",
        "city": "Anytown",
        "country": "USA",
        "zip": "12345",
        "date": "2024-01-26T17:16:19-0800",
        "leadStatus": "New",
        "leadType": "Internet",
        "providerName": "Momentum",
        "comments": "Test lead",
        "vehicleType": "New",
        "vin": "1HGCM82633A123456",
        "stock": "STOCK123",
        "make": "Honda",
        "model": "Accord",
        "year": "2023",
        "color": "Blue",
        "trim": "EX-L",
        "bdcID": "BDC123",
        "bdcName": "Jane Smith"
    }

    result = parse_lead(product_dealer_id, data)

    assert result["product_dealer_id"] == product_dealer_id
    assert result["crm_dealer_id"] == "DEALER456"
    
    # Verify consumer data
    consumer = result["consumer"]
    assert consumer["first_name"] == "John"
    assert consumer["last_name"] == "Doe"
    assert consumer["crm_consumer_id"] == "CONSUMER789"
    assert consumer["phone"] == "1234567890"
    assert consumer["email"] == "john.doe@example.com"
    assert consumer["address"] == "123 Main St"
    assert consumer["city"] == "Anytown"
    assert consumer["country"] == "USA"
    assert consumer["postal_code"] == "12345"

    # Verify lead data
    lead = result["lead"]
    assert lead["crm_lead_id"] == "LEAD123"
    assert lead["lead_status"] == "New"
    assert lead["lead_origin"] == "Internet"
    assert lead["lead_source"] == "Momentum"
    assert lead["lead_comment"] == "Test lead"

    # Verify vehicle data
    vehicle = result["vehicle"]
    assert vehicle["vin"] == "1HGCM82633A123456"
    assert vehicle["stock_num"] == "STOCK123"
    assert vehicle["make"] == "Honda"
    assert vehicle["model"] == "Accord"
    assert vehicle["year"] == 2023
    assert vehicle["exterior_color"] == "Blue"
    assert vehicle["trim"] == "EX-L"
    assert vehicle["condition"] == "New"

    # Verify salesperson data
    salesperson = result["salesperson"]
    assert salesperson["crm_salesperson_id"] == "BDC123"
    assert salesperson["first_name"] == "Jane"
    assert salesperson["last_name"] == "Smith"
    assert salesperson["position_name"] == "BDC Rep"


# Edge Cases
def test_parse_lead_missing_optional_fields():
    """Test parsing with missing optional fields.
    
    Verifies that the transformation handles cases where:
    - Optional fields are missing
    - The output still maintains the correct structure
    """
    product_dealer_id = "TEST123"
    data = {
        "id": "LEAD123",
        "dealerID": "DEALER456",
        "firstName": "John",
        "lastName": "Doe",
        "personApiID": "CONSUMER789",
        "leadStatus": "New",
        "leadType": "Internet"
    }

    result = parse_lead(product_dealer_id, data)

    assert result["product_dealer_id"] == product_dealer_id
    assert result["crm_dealer_id"] == "DEALER456"
    
    # Verify consumer data has only required fields
    consumer = result["consumer"]
    assert consumer["first_name"] == "John"
    assert consumer["last_name"] == "Doe"
    assert consumer["crm_consumer_id"] == "CONSUMER789"
    assert "phone" not in consumer
    assert "email" not in consumer
    assert "address" not in consumer

    # Verify lead data has only required fields
    lead = result["lead"]
    assert lead["crm_lead_id"] == "LEAD123"
    assert lead["lead_status"] == "New"
    assert lead["lead_origin"] == "Internet"
    assert lead["lead_source"] == ""
    assert lead["lead_comment"] == ""

    # Verify vehicle and salesperson are empty
    assert result["vehicle"] == {}
    assert result["salesperson"] == {}


# Error Cases
@patch("transform_new_lead.requests.post")
def test_create_consumer_error(mock_post):
    """Test consumer creation error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    """
    mock_post.side_effect = Exception("API Error")
    
    parsed_lead = {
        "product_dealer_id": "TEST123",
        "consumer": {
            "first_name": "John",
            "last_name": "Doe"
        }
    }
    
    with pytest.raises(Exception) as exc_info:
        create_consumer(parsed_lead, "test-api-key")
    
    assert "Error creating consumer from CRM API" in str(exc_info.value)


@patch("transform_new_lead.requests.post")
def test_create_lead_error(mock_post):
    """Test lead creation error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    """
    mock_post.side_effect = Exception("API Error")
    
    parsed_lead = {
        "lead": {
            "crm_lead_id": "LEAD123",
            "lead_status": "New"
        }
    }
    
    with pytest.raises(Exception) as exc_info:
        create_lead(parsed_lead, "CONSUMER123", "test-api-key")
    
    assert "Error creating lead from CRM API" in str(exc_info.value)


# Integration Tests
@patch("transform_new_lead.s3_client.get_object")
@patch("transform_new_lead.sm_client.get_secret_value")
@patch("transform_new_lead.requests.get")
@patch("transform_new_lead.requests.post")
@patch("transform_new_lead.MomentumApiWrapper")
def test_lambda_handler_success(
    mock_momentum_api,
    mock_post,
    mock_get,
    mock_get_secret,
    mock_s3_get,
    mock_sqs_event,
    mock_s3_data,
    mock_secret,
    mock_api_responses,
    caplog
):
    """Test complete lambda handler success path.
    
    Verifies the entire workflow:
    1. SQS event processing
    2. Data retrieval from S3
    3. Secret retrieval from Secrets Manager
    4. Data transformation
    5. API calls to CRM
    6. Proper logging
    7. Success response generation
    """
    # Setup S3 mock
    mock_s3_get.return_value = {
        "Body": MagicMock(
            read=lambda: json.dumps(mock_s3_data).encode("utf-8")
        )
    }

    # Setup Secrets Manager mock
    mock_get_secret.return_value = mock_secret

    # Setup Momentum API mock
    mock_momentum_api.return_value.get_alternate_person_ids.return_value = {
        "personApiID": None,
        "mergedPersonApiIDs": []
    }

    # Setup CRM API mocks
    mock_get.side_effect = [
        mock_api_responses["get_existing_lead"],
        mock_api_responses["get_existing_consumer"],
        mock_api_responses["get_recent_leads"]
    ]
    mock_post.side_effect = [
        mock_api_responses["create_consumer"],
        mock_api_responses["create_lead"]
    ]

    # Execute lambda handler
    with caplog.at_level(logging.INFO):
        result = lambda_handler(mock_sqs_event, None)

    # Verify S3 interaction
    mock_s3_get.assert_called_once_with(
        Bucket="test-bucket",
        Key="momentum/1234.TEST/leads.json"
    )

    # Verify Secrets Manager interaction
    mock_get_secret.assert_called_once()

    # Verify CRM API interactions
    assert mock_get.call_count == 3  # existing lead, existing consumer, recent leads
    assert mock_post.call_count == 2  # create consumer, create lead

    # Verify logging
    assert "New lead created: NEW_LEAD123" in caplog.text

    # Verify result
    assert result["batchItemFailures"] == []


@patch("transform_new_lead.s3_client.get_object")
@patch("transform_new_lead.sm_client.get_secret_value")
@patch("transform_new_lead.requests.get")
@patch("transform_new_lead.requests.post")
@patch("transform_new_lead.MomentumApiWrapper")
def test_lambda_handler_duplicate_lead(
    mock_momentum_api,
    mock_post,
    mock_get,
    mock_get_secret,
    mock_s3_get,
    mock_sqs_event,
    mock_s3_data,
    mock_secret,
    mock_api_responses,
    caplog
):
    """Test lambda handler with duplicate lead detection.
    
    Verifies that:
    1. Duplicate leads are properly detected
    2. Appropriate warning messages are logged
    3. The lead is not created
    """
    # Setup S3 mock
    mock_s3_get.return_value = {
        "Body": MagicMock(
            read=lambda: json.dumps(mock_s3_data).encode("utf-8")
        )
    }

    # Setup Secrets Manager mock
    mock_get_secret.return_value = mock_secret

    # Setup Momentum API mock
    mock_momentum_api.return_value.get_alternate_person_ids.return_value = {
        "personApiID": None,
        "mergedPersonApiIDs": []
    }

    # Setup CRM API mocks to simulate existing lead
    mock_get.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {"lead_id": "EXISTING_LEAD123"}
        ),
        mock_api_responses["get_existing_consumer"],
        mock_api_responses["get_recent_leads"]
    ]

    # Execute lambda handler
    with caplog.at_level(logging.WARNING):
        result = lambda_handler(mock_sqs_event, None)

    # Verify warning message
    assert "Existing lead detected: DB Lead ID EXISTING_LEAD123" in caplog.text

    # Verify no consumer or lead creation
    assert mock_post.call_count == 0

    # Verify result
    assert result["batchItemFailures"] == [] 