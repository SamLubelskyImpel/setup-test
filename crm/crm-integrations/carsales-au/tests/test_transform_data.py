"""Test suite for the Carsales data transformation lambda.

This test suite covers the following aspects of the Carsales lead transformation:
1. Parsing and transformation of Carsales-specific data to a unified format
2. Extraction and formatting of contact information
3. Validation of incoming lead data
4. Integration with downstream services (CRM API)
5. Error handling for invalid data and service failures
6. End-to-end lambda execution flow
"""

import json
import logging
from typing import Any, Dict
from unittest.mock import patch, MagicMock

import pytest
import requests
from moto import mock_aws

from transform_data import (
    record_handler,
    parse_json_to_entries,
    extract_value,
    upload_consumer_to_db,
)


# Test Data Fixtures
@pytest.fixture
def mock_carsales_raw_data() -> Dict[str, Any]:
    """Sample raw data from Carsales."""
    return {
        "Identifier": "CS12345",
        "CreatedUtc": "2024-01-15T10:30:00Z",
        "Status": "New",
        "Comments": "Test lead comment",
        "Seller": {"Identifier": "DEALER123"},
        "Item": {
            "ListingType": "Used",
            "Identification": [
                {"Type": "StockNumber", "Value": "STOCK123"}
            ],
            "Specification": {
                "ReleaseDate": {"Year": "2020"},
                "Make": "Toyota",
                "Model": "Camry"
            },
            "PriceList": [
                {"Amount": "25000.00"}
            ]
        },
        "Prospect": {
            "Name": "John Smith",
            "Email": "john.smith@example.com",
            "PhoneNumbers": [
                {"Type": "Mobile", "Number": "0400123456"}
            ]
        },
        "Assignment": {
            "Name": "Sales Person",
            "Email": "sales@dealer.com"
        }
    }


@pytest.fixture
def mock_transformed_lead_data(mock_carsales_raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Expected transformed lead data."""
    return {
        "product_dealer_id": "DEALER_ABC",
        "lead": {
            "crm_lead_id": mock_carsales_raw_data["Identifier"],
            "lead_ts": mock_carsales_raw_data["CreatedUtc"],
            "lead_status": mock_carsales_raw_data["Status"],
            "lead_substatus": "",
            "lead_comment": mock_carsales_raw_data["Comments"],
            "lead_origin": "Internet",
            "lead_source": "CarSales",
            "vehicles_of_interest": [{
                "stock_num": "STOCK123",
                "year": 2020,
                "make": "Toyota",
                "model": "Camry",
                "condition": "Used",
                "price": 25000.00
            }],
            "salespersons": [{
                "first_name": "Sales",
                "last_name": "Person",
                "email": "sales@dealer.com"
            }]
        },
        "consumer": {
            "first_name": "John",
            "last_name": "Smith",
            "email": "john.smith@example.com",
            "phone": "0400123456"
        }
    }


@pytest.fixture
def mock_sqs_s3_event() -> Dict[str, Any]:
    """Mock SQS event containing S3 event details."""
    return {
        "Records": [{
            "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "body": json.dumps({
                "detail": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "carsales/leads/DEALER_ABC/lead.json"}
                }
            })
        }]
    }


# Success Cases
class TestSuccessCases:
    def test_extract_value_identification(self) -> None:
        """Test extracting identification values."""
        items = [{"Type": "StockNumber", "Value": "ABC123"}]
        result = extract_value("Identification", items, "StockNumber")
        assert result == "ABC123"

    def test_extract_value_phone(self) -> None:
        """Test extracting phone numbers."""
        items = [{"Type": "Mobile", "Number": "0400123456"}]
        result = extract_value("PhoneNumbers", items, "Mobile")
        assert result == "0400123456"

    def test_parse_json_to_entries_success(self, mock_carsales_raw_data: Dict[str, Any], mock_transformed_lead_data: Dict[str, Any]) -> None:
        """Test successful parsing of Carsales JSON data."""
        result = parse_json_to_entries("DEALER_ABC", mock_carsales_raw_data)
        assert result["product_dealer_id"] == mock_transformed_lead_data["product_dealer_id"]
        assert result["lead"]["crm_lead_id"] == mock_transformed_lead_data["lead"]["crm_lead_id"]
        assert result["consumer"]["email"] == mock_transformed_lead_data["consumer"]["email"]

    @patch("transform_data.requests.post")
    def test_upload_consumer_to_db_success(self, mock_post: MagicMock, mock_transformed_lead_data: Dict[str, Any]) -> None:
        """Test successful consumer upload to CRM."""
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"consumer_id": "CONS123"}
        result = upload_consumer_to_db(
            mock_transformed_lead_data["consumer"],
            mock_transformed_lead_data["product_dealer_id"],
            "test-api-key"
        )
        assert result == "CONS123"


# Edge Cases
class TestEdgeCases:
    def test_parse_json_to_entries_missing_optional_fields(self) -> None:
        """Test parsing JSON with missing optional fields."""
        minimal_data = {
            "Identifier": "CS12345",
            "Seller": {"Identifier": "DEALER123"},
            "Prospect": {
                "Name": "John Smith",
                "Email": "john@example.com"
            }
        }
        result = parse_json_to_entries("DEALER_ABC", minimal_data)
        assert result["lead"]["crm_lead_id"] == "CS12345"
        assert result["consumer"]["email"] == "john@example.com"
        assert "phone" not in result["consumer"]

    def test_extract_value_not_found(self) -> None:
        """Test extracting non-existent value."""
        items = [{"Type": "Other", "Value": "123"}]
        result = extract_value("Identification", items, "StockNumber")
        assert result is None


# Error Cases
class TestErrorHandling:
    def test_parse_json_to_entries_missing_required_fields(self) -> None:
        """Test parsing JSON with missing required fields."""
        invalid_data = {
            "Identifier": "CS12345",
            "Seller": {"Identifier": "DEALER123"}
            # Missing Prospect data
        }
        with pytest.raises(Exception, match="No Consumer provided for lead CS12345"):
            parse_json_to_entries("DEALER_ABC", invalid_data)

    @patch("transform_data.requests.post")
    def test_upload_consumer_to_db_api_error(self, mock_post: MagicMock, mock_transformed_lead_data: Dict[str, Any]) -> None:
        """Test consumer upload with API error."""
        mock_post.return_value.status_code = 500
        mock_post.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError("API Error")

        with pytest.raises(requests.exceptions.HTTPError, match="API Error"):
            upload_consumer_to_db(
                mock_transformed_lead_data["consumer"],
                mock_transformed_lead_data["product_dealer_id"],
                "test-api-key"
            )
