"""Test suite for the Activix lead transformation lambda.

This test suite covers the following aspects of the Activix lead transformation:
1. Parsing and validation of Activix lead data
2. Phone and email parsing/validation
3. Address formatting
4. Salesperson data extraction
5. Integration with CRM API and AWS services
6. Error handling and validation
7. End-to-end lambda execution flow
"""

import json
import logging
from typing import Any, Dict
from unittest.mock import patch, MagicMock

import pytest
from moto import mock_aws
from transform_new_lead import (
    lambda_handler,
    parse_lead,
    parse_phone_number,
    parse_email,
    parse_address,
    extract_salesperson,
    get_existing_lead,
    create_consumer,
)



# Fixtures
@pytest.fixture
def mock_sqs_s3_event() -> Dict[str, Any]:
    """Mock SQS event containing an S3 event notification in its body."""
    s3_event_notification = {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2023-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "detail": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {"name": "test-bucket-leads", "arn": "arn:aws:s3:::test-bucket-leads"},
                    "object": {"key": "activix/leads/dealer123/test_lead_file.json", "size": 1024}
                }
    }
    return {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": json.dumps(s3_event_notification),
                "attributes": {"ApproximateReceiveCount": "1", "SentTimestamp": "1545082649183"},
                "eventSource": "aws:sqs",
                "awsRegion": "us-east-1"
            }
        ]
    }


@pytest.fixture
def mock_activix_lead_data() -> Dict[str, Any]:
    """Mock raw lead data as it would appear in an S3 file from Activix."""
    return {
        "id": "12345",
        "account_id": "12345",
        "customer_id": "C789",
        "first_name": "John",
        "last_name": "Doe",
        "created_at": "2024-03-15T10:30:00+00:00",
        "phones": [
            {"type": "mobile", "number": "555-123-4567", "valid": True},
            {"type": "home", "number": "555-987-6543", "valid": True}
        ],
        "emails": [
            {"address": "john.doe@example.com", "valid": True},
            {"address": "jdoe@work.com", "valid": False}
        ],
        "address_line1": "123 Main St",
        "address_line2": "Apt 4B",
        "city": "Springfield",
        "country": "USA",
        "postal_code": "12345",
        "status": "NEW",
        "type": "WEBSITE",
        "source": "ORGANIC",
        "provider": "DEALER_WEBSITE",
        "comment": "Interested in new vehicles",
        "vehicles": [
            {
                "id": "V123",
                "type": "wanted",
                "vin": "1HGCM82633A123456",
                "stock": "ST123",
                "make": "Honda",
                "model": "Accord",
                "year": "2024",
                "odometer": 10,
                "transmission": "Automatic",
                "color_interior": "Black",
                "color_exterior": "Silver",
                "trim": "Sport",
                "price": 32000,
                "comment": "Preferred vehicle"
            },
            {
                "id": "V456",
                "type": "exchange",
                "vin": "5YJSA1E27JF123456",
                "make": "Tesla",
                "model": "Model S",
                "year": "2020"
            }
        ],
        "bdc": {
            "id": "B123",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane.smith@dealer.com"
        }
    }


@pytest.fixture
def mock_secrets_manager_response() -> Dict[str, Any]:
    """Mock Secrets Manager `get_secret_value` response."""
    inner_secret_payload = json.dumps({
        "api_key": "test-api-key-12345",
        "api_endpoint": "https://api.crm.example.com/v1"
    })
    outer_secret_payload = {
        "impel": inner_secret_payload,
        "other_config": "some_value"
    }
    return outer_secret_payload


# Test Cases
class TestSuccessCases:
    def test_parse_phone_number_mobile_preferred(self) -> None:
        """Test that mobile phone is preferred when available."""
        phones = [
            {"type": "home", "number": "555-111-2222", "valid": True},
            {"type": "mobile", "number": "555-333-4444", "valid": True}
        ]
        assert parse_phone_number(phones) == "555-333-4444"

    def test_parse_email_valid_preferred(self) -> None:
        """Test that valid email is preferred."""
        emails = [
            {"address": "invalid@test.com", "valid": False},
            {"address": "valid@test.com", "valid": True}
        ]
        assert parse_email(emails) == "valid@test.com"

    def test_parse_address_both_lines(self) -> None:
        """Test address parsing with both lines present."""
        data = {"address_line1": "123 Main St", "address_line2": "Apt 4B"}
        assert parse_address(data) == "123 Main St Apt 4B"

    def test_extract_salesperson_bdc(self) -> None:
        """Test salesperson extraction for BDC role."""
        bdc_data = {
            "id": "B123",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane@dealer.com"
        }
        expected = {
            "crm_salesperson_id": "B123",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane@dealer.com",
            "is_primary": True,
            "position_name": "BDC"
        }
        assert extract_salesperson("BDC", bdc_data) == expected

    def test_parse_lead_complete(self, mock_activix_lead_data: Dict[str, Any]) -> None:
        """Test complete lead parsing with all fields."""
        product_dealer_id = "dealer123"
        parsed = parse_lead(product_dealer_id, mock_activix_lead_data)
        assert parsed["product_dealer_id"] == product_dealer_id
        assert parsed["consumer"]["first_name"] == "John"
        assert parsed["consumer"]["last_name"] == "Doe"
        assert parsed["consumer"]["phone"] == "555-123-4567"  # Mobile preferred
        assert parsed["consumer"]["email"] == "john.doe@example.com"  # Valid email preferred
        assert parsed["consumer"]["address"] == "123 Main St Apt 4B"
        assert parsed["lead"]["crm_lead_id"] == "12345"
        assert len(parsed["vehicles"]) == 2
        assert parsed["vehicles"][0]["vin"] == "1HGCM82633A123456"  # Wanted vehicle first
        assert parsed["salesperson"]["crm_salesperson_id"] == "B123"


class TestEdgeCases:
    def test_parse_phone_number_no_mobile(self) -> None:
        """Test phone parsing when no mobile number is available."""
        phones = [
            {"type": "home", "number": "555-111-2222", "valid": True},
            {"type": "work", "number": "555-333-4444", "valid": True}
        ]
        assert parse_phone_number(phones) == "555-111-2222"

    def test_parse_phone_number_empty_list(self) -> None:
        """Test phone parsing with empty list."""
        assert parse_phone_number([]) is None

    def test_parse_email_all_invalid(self) -> None:
        """Test email parsing when all emails are invalid."""
        emails = [
            {"address": "invalid1@test.com", "valid": False},
            {"address": "invalid2@test.com", "valid": False}
        ]
        assert parse_email(emails) == "invalid1@test.com"

    def test_parse_address_single_line(self) -> None:
        """Test address parsing with only one line."""
        data = {"address_line1": "123 Main St", "address_line2": None}
        assert parse_address(data) == "123 Main St"

    def test_parse_lead_minimal(self) -> None:
        """Test lead parsing with minimal required fields."""
        minimal_data = {
            "id": "12345",
            "customer_id": "C789",
            "first_name": "John",
            "last_name": "Doe",
            "created_at": "2024-03-15T10:30:00+00:00",
            "emails": [{"address": "john@test.com", "valid": True}],
            "phones": [{"type": "mobile", "number": "555-123-4567", "valid": True}]
        }
        parsed = parse_lead("dealer123", minimal_data)
        assert parsed["consumer"]["email"] == "john@test.com"
        assert parsed["consumer"]["phone"] == "555-123-4567"
        assert "vehicles" in parsed
        assert parsed["vehicles"] == []


class TestErrorHandlingAndValidation:
    @patch("requests.get")
    def test_get_existing_lead_not_found(self, mock_get: MagicMock) -> None:
        """Test getting non-existent lead."""
        mock_get.return_value.status_code = 404
        result = get_existing_lead("12345", "dealer123", "test-api-key")
        assert result is None

    @patch("requests.get")
    def test_get_existing_lead_error(self, mock_get: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        """Test error handling when getting existing lead."""
        mock_get.return_value.status_code = 500
        mock_get.return_value.text = "Internal Server Error"
        with caplog.at_level(logging.ERROR):
            with pytest.raises(Exception, match="Error getting existing lead from CRM API"):
                get_existing_lead("12345", "dealer123", "test-api-key")
        assert "Error getting existing lead from CRM API" in caplog.text

    @patch("requests.post")
    def test_create_consumer_error(self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        """Test error handling when creating consumer."""
        mock_post.side_effect = Exception("API Error")
        parsed_lead = {
            "consumer": {"first_name": "John", "last_name": "Doe"},
            "product_dealer_id": "dealer123"
        }
        with caplog.at_level(logging.ERROR):
            with pytest.raises(Exception, match="API Error"):
                create_consumer(parsed_lead, "test-api-key")
        assert "Error creating consumer from CRM API: API Error" in caplog.text


@mock_aws
class TestIntegration:
    @patch("requests.post")
    @patch("requests.get")
    def test_lambda_handler_successful_flow(
        self,
        mock_get: MagicMock,
        mock_post: MagicMock,
        mock_sqs_s3_event: Dict[str, Any],
        mock_activix_lead_data: Dict[str, Any],
        mock_secrets_manager_response: Dict[str, Any],
        caplog: pytest.LogCaptureFixture,
        s3_client,
        secretsmanager_client
    ) -> None:
        """Test successful end-to-end lambda execution."""
        # Setup S3
        s3_event_body = json.loads(mock_sqs_s3_event["Records"][0]["body"])
        s3_details = s3_event_body["detail"]
        bucket_name = s3_details["bucket"]["name"]
        object_key = s3_details["object"]["key"]
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(mock_activix_lead_data).encode('utf-8')
        )

        # Setup Secrets Manager with correct secret name based on environment
        secret_name = "test/crm-api"
        secretsmanager_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(mock_secrets_manager_response)
        )

        # Mock API responses
        mock_get.return_value.status_code = 404  # Lead doesn't exist
        mock_post.return_value.status_code = 201
        mock_post.return_value.json.side_effect = [
            {"consumer_id": "C123"},  # create_consumer response
            {"lead_id": "L456"}       # create_lead response
        ]

        # Execute lambda
        with patch("boto3.client") as mock_boto_client:
            def boto_client_side_effect(service_name, *args, **kwargs):
                if service_name == 's3':
                    return s3_client
                elif service_name == 'secretsmanager':
                    return secretsmanager_client
                return MagicMock()
            mock_boto_client.side_effect = boto_client_side_effect
            response = lambda_handler(mock_sqs_s3_event, MagicMock())

        # Verify success
        print(response)
        assert not response["batchItemFailures"]
        assert mock_post.call_count == 2  # One for consumer, one for lead
        assert "New lead created" in caplog.text

    @patch("requests.post")
    @patch("requests.get")
    def test_lambda_handler_duplicate_lead(
        self,
        mock_get: MagicMock,
        mock_post: MagicMock,
        mock_sqs_s3_event: Dict[str, Any],
        mock_activix_lead_data: Dict[str, Any],
        mock_secrets_manager_response: Dict[str, Any],
        caplog: pytest.LogCaptureFixture,
        s3_client,
        secretsmanager_client
    ) -> None:
        """Test handling of duplicate lead."""
        # Setup same as successful flow, but mock get_existing_lead to return an existing lead
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"lead_id": "EXISTING_123"}

        s3_event_body = json.loads(mock_sqs_s3_event["Records"][0]["body"])
        bucket_name = s3_event_body["detail"]["bucket"]["name"]
        object_key = s3_event_body["detail"]["object"]["key"]
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(mock_activix_lead_data).encode('utf-8')
        )
        # Setup Secrets Manager with correct secret name based on environment
        secret_name = "test/crm-api"
        secretsmanager_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(mock_secrets_manager_response)
        )

        with patch("boto3.client") as mock_boto_client:
            def boto_client_side_effect(service_name, *args, **kwargs):
                if service_name == 's3':
                    return s3_client
                elif service_name == 'secretsmanager':
                    return secretsmanager_client
                return MagicMock()
            mock_boto_client.side_effect = boto_client_side_effect
            response = lambda_handler(mock_sqs_s3_event, MagicMock())

        assert not response["batchItemFailures"]
        assert mock_post.call_count == 0  # No API calls should be made
        assert "Ignoring duplicate lead" in caplog.text
