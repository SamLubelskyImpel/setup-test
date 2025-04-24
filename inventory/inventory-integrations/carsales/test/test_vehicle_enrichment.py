import json
import os
import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../app"))
sys.path.append(app_dir)
os.environ["ENVIRONMENT"] = "test"

from vehicle_enrichment import record_handler, get_redbook_data, get_vdp_data


class TestVehicleEnrichment:
    """Test cases for the vehicle_enrichment module."""

    @pytest.fixture(scope="class")
    def mock_s3_client(self):
        """This fixture mocks the S3 client."""
        with patch("vehicle_enrichment.s3_client") as mock_s3:
            yield mock_s3

    @pytest.fixture(scope="function")
    def mock_lambda_client(self):
        """This fixture mocks the Lambda client."""
        with patch("vehicle_enrichment.lambda_client") as mock_lambda:
            yield mock_lambda

    @pytest.fixture(scope="class")
    def sqs_record(self):
        """This fixture returns an SQS record."""
        return SQSRecord(
            {
                "body": json.dumps(
                    {
                        "Records": [
                            {
                                "s3": {
                                    "bucket": {"name": "test-bucket"},
                                    "object": {
                                        "key": "raw/carsales/dealer123/vehicle.json"
                                    },
                                }
                            }
                        ]
                    }
                )
            }
        )

    def test_record_handler_success(
        self, mock_s3_client, mock_lambda_client, sqs_record
    ):
        """Test record_handler function."""
        # Mock S3 get_object response
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=json.dumps(
                        {
                            "Specification": {
                                "SpecificationSource": "REDBOOK",
                                "SpecificationCode": "12345",
                            },
                            "Identification": [{"Type": "VIN", "Value": "VIN123"}],
                        }
                    ).encode("utf-8")
                )
            )
        }

        # Mock RedBook Lambda response
        mock_lambda_client.invoke.side_effect = [
            {
                "Payload": MagicMock(
                    read=MagicMock(
                        return_value=json.dumps(
                            json.dumps({"results": [{"key": "value"}]})
                        ).encode("utf-8")
                    )
                )
            },
            {
                "Payload": MagicMock(
                    read=MagicMock(
                        return_value=json.dumps(
                            {
                                "statusCode": 200,
                                "body": json.dumps(
                                    {
                                        "VDP URL": "http://example.com",
                                        "SRP IMAGE URL": "http://image.com",
                                    }
                                ),
                            }
                        ).encode("utf-8")
                    )
                )
            }
        ]

        # Call the record_handler
        record_handler(sqs_record)

        # Assertions
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="raw/carsales/dealer123/vehicle.json"
        )
        assert mock_lambda_client.invoke.call_count == 2
        mock_s3_client.put_object.assert_called_once()

    def test_get_redbook_data_success(self, mock_lambda_client):
        """Test get_redbook_data function."""
        vehicle_data = {
            "Specification": {"SpecificationSource": "REDBOOK", "SpecificationCode": "12345"}
        }

        # Mock RedBook Lambda response
        mock_lambda_client.invoke.return_value = {
                "Payload": MagicMock(
                    read=MagicMock(
                        return_value=json.dumps(
                            json.dumps({"results": [{"key": "value"}]})
                        ).encode("utf-8")
                    )
                )
        }

        redbook_data = get_redbook_data(vehicle_data)

        # Assertions
        assert "results" in redbook_data
        assert redbook_data["results"][0]["key"] == "value"

    def test_get_vdp_data_success(self, mock_lambda_client):
        """Test get_vdp_data function."""
        vehicle_data = {
            "Identification": [
                {"Type": "VIN", "Value": "VIN123"},
                {"Type": "StockNumber", "Value": "STOCK123"},
            ]
        }
        impel_dealer_id = "dealer123"

        # Mock VDP Lambda response
        mock_lambda_client.invoke.return_value = {
            "Payload": MagicMock(
                read=MagicMock(
                    return_value=json.dumps(
                        {"statusCode": 200, "body": json.dumps({"VDP URL": "http://example.com", "SRP IMAGE URL": "http://image.com"})}
                    ).encode("utf-8")
                )
            )
        }

        vdp_data = get_vdp_data(vehicle_data, impel_dealer_id)

        # Assertions
        assert vdp_data["VDP URL"] == "http://example.com"
        assert vdp_data["SRP IMAGE URL"] == "http://image.com"

    def test_get_vdp_data_failure(self, mock_lambda_client):
        """Test get_vdp_data function when it fails."""
        vehicle_data = {
            "Identification": [
                {"Type": "VIN", "Value": "VIN123"},
                {"Type": "StockNumber", "Value": "STOCK123"},
            ]
        }
        impel_dealer_id = "dealer123"

        # Mock VDP Lambda response with a 500 error
        mock_lambda_client.invoke.return_value = {
            "Payload": MagicMock(
                read=MagicMock(
                    return_value=json.dumps(
                        {"statusCode": 500, "body": json.dumps({"error": "VDP service failed"})}
                    ).encode("utf-8")
                )
            )
        }

        with pytest.raises(Exception, match="VDP service failed"):
            get_vdp_data(vehicle_data, impel_dealer_id)

        # Mock VDP Lambda response with a 404 error
        mock_lambda_client.invoke.return_value = {
            "Payload": MagicMock(
                read=MagicMock(
                    return_value=json.dumps(
                        {"statusCode": 404, "body": json.dumps({"error": "VDP not found"})}
                    ).encode("utf-8")
                )
            )
        }

        # Verify that an empty dictionary is returned for a 404 response
        vdp_data = get_vdp_data(vehicle_data, impel_dealer_id)
        assert vdp_data == {}
