"""Test suite for the DealerPeak send_activity lambda.

This test suite covers aspects of processing SQS messages to send activity
data to DealerPeak, including:
1.  SQS message parsing and data extraction.
2.  Interaction with CrmApiWrapper for salesperson data and activity updates.
3.  Interaction with DealerpeakApiWrapper for creating activities.
4.  Error handling for API failures and invalid data.
5.  Batch processing in the lambda_handler.
"""

import json
import logging

import pytest

# AWS Lambda Powertools imports (still needed for SQS types)
from aws_lambda_powertools.utilities.batch import EventType
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

# Functions and classes to test/mock - imported from send_activity directly
from send_activity import (
    CRMApiError,  # Exception class
    lambda_handler,
)
from send_activity import (
    record_handler as send_activity_record_handler,  # aliased
)

MODULE_PATH = "send_activity"  # For patching module-level items


def test_record_handler_success(
    caplog,
    mock_crm_api,
    mock_dealerpeak_api,
    sqs_record,
    mock_activity_data,
    mock_salesperson_data,
):
    """Test successful processing of a single SQS record by record_handler."""
    # Execute the function under test
    send_activity_record_handler(sqs_record, mock_crm_api)

    # Assertions
    mock_crm_api.get_salesperson.assert_called_once_with(mock_activity_data["lead_id"])
    mock_dealerpeak_api.assert_called_once_with(
        activity=mock_activity_data, salesperson=mock_salesperson_data
    )
    mock_dealerpeak_api.return_value.create_activity.assert_called_once()
    mock_crm_api.update_activity.assert_called_once_with(
        mock_activity_data["activity_id"], "dp_task_id_123"
    )

    # Log assertions
    assert "Record:" in caplog.text
    assert f"'message_id': '{sqs_record.message_id}'" in caplog.text
    assert f"'body': '{sqs_record.body}'" in caplog.text
    assert "Dealerpeak responded with task ID: dp_task_id_123" in caplog.text


def test_record_handler_crm_api_error_get_salesperson(
    caplog, mock_crm_api, mock_dealerpeak_api, sqs_record
):
    """Test record_handler when CrmApiWrapper.get_salesperson raises CRMApiError."""
    mock_crm_api.get_salesperson.side_effect = CRMApiError("Failed to get salesperson")

    result = send_activity_record_handler(sqs_record, mock_crm_api)
    assert result is None

    mock_crm_api.get_salesperson.assert_called_once()
    mock_dealerpeak_api.assert_not_called()
    mock_crm_api.update_activity.assert_not_called()


def test_record_handler_crm_api_error_update_activity(
    caplog, mock_crm_api, mock_dealerpeak_api, sqs_record
):
    """Test record_handler when CrmApiWrapper.update_activity raises CRMApiError."""
    mock_crm_api.update_activity.side_effect = CRMApiError("Failed to update activity")

    result = send_activity_record_handler(sqs_record, mock_crm_api)
    assert result is None

    mock_crm_api.get_salesperson.assert_called_once()
    mock_dealerpeak_api.assert_called_once()
    mock_dealerpeak_api.return_value.create_activity.assert_called_once()
    mock_crm_api.update_activity.assert_called_once()


def test_record_handler_dealerpeak_api_error_create_activity(
    caplog, mock_crm_api, mock_dealerpeak_api, sqs_record, mock_activity_data
):
    """Test record_handler when DealerpeakApiWrapper.create_activity raises an Exception."""
    dealerpeak_error_message = "Dealerpeak connection failed during create"
    mock_dealerpeak_api.return_value.create_activity.side_effect = Exception(
        dealerpeak_error_message
    )

    with pytest.raises(Exception, match=dealerpeak_error_message):
        send_activity_record_handler(sqs_record, mock_crm_api)

    mock_crm_api.get_salesperson.assert_called_once()
    mock_dealerpeak_api.assert_called_once()
    mock_dealerpeak_api.return_value.create_activity.assert_called_once()
    mock_crm_api.update_activity.assert_not_called()

    assert (
        f"Failed to post activity {mock_activity_data.get('activity_id', 'N/A')} to Dealerpeak"
        in caplog.text
    )
    assert "[SUPPORT ALERT] Failed to Send Activity" in caplog.text
    assert dealerpeak_error_message in caplog.text


def test_lambda_handler_success(
    caplog,
    mock_crm_api,
    mock_dealerpeak_api,
    mock_sqs_record_data,
    mock_activity_data,
    mock_salesperson_data,
    mocker,
):
    """Test successful invocation of lambda_handler."""
    mock_sqs_event = {"Records": [mock_sqs_record_data]}
    context = mocker.MagicMock()

    result = lambda_handler(mock_sqs_event, context)

    assert result["batchItemFailures"] == []
    assert "Record:" in caplog.text
    assert f"Event: {mock_sqs_event}" in caplog.text

    # Verify the API calls were made
    mock_crm_api.get_salesperson.assert_called_once_with(mock_activity_data["lead_id"])
    mock_dealerpeak_api.assert_called_once_with(
        activity=mock_activity_data, salesperson=mock_salesperson_data
    )
    mock_dealerpeak_api.return_value.create_activity.assert_called_once()
    mock_crm_api.update_activity.assert_called_once_with(
        mock_activity_data["activity_id"], "dp_task_id_123"
    )
