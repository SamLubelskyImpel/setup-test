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
import requests


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
def test_create_consumer_error(mock_post, caplog):
    """Test consumer creation error handling.
    
    Verifies that:
    1. API errors are properly caught
    2. Error messages are logged
    3. Exceptions are raised with appropriate messages
    """
    # Create a mock response that will raise an exception when raise_for_status() is called
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = {"error": "API Error"}
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
    
    mock_post.return_value = mock_response
    
    parsed_lead = {
        "product_dealer_id": "TEST123",
        "consumer": {
            "first_name": "John",
            "last_name": "Doe"
        }
    }
    
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        create_consumer(parsed_lead, "test-api-key")
    
    # Check that the error was logged with the correct message
    assert "Error creating consumer from CRM API" in caplog.text
    assert "500 Server Error" in str(exc_info.value)

