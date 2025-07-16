"""Test suite for the DealerPeak CloudWatch Logs monitoring lambda.

This test suite covers:
1.  Decoding of gzipped and base64 encoded CloudWatch Logs events.
2.  Processing of log messages to extract support alerts.
3.  Sending notifications via SNS for support alerts.
4.  End-to-end lambda handler logic for processing log events.
"""

import pytest
import json
import base64
import gzip
import logging
from moto import mock_aws

# Components to test from monitoring.py
from monitoring import (
    decode_logs_event,
    process_support_log,
    send_alert_notification,
    lambda_handler
)

MODULE_PATH = "monitoring" # For patching module-level items


@pytest.fixture(scope="module")
def mock_log_message_content():
    """Mock log message content used in tests."""
    return "[SUPPORT ALERT] Critical System Failure [CONTENT] Traceback: Error X occurred at Y. Details: Z."


@pytest.fixture(scope="module")
def mock_log_stream_name():
    """Mock log stream name used in tests."""
    return "test-log-group/test-log-stream"


@pytest.fixture(scope="module")
def mock_decoded_logs_data(mock_log_message_content, mock_log_stream_name):
    """Mock decoded log data structure that decode_logs_event would return."""
    log_group, log_stream = mock_log_stream_name.split('/', 1)
    return {
        "messageType": "DATA_MESSAGE",
        "owner": "123456789012",
        "logGroup": log_group,
        "logStream": log_stream,
        "subscriptionFilters": ["TestFilter"],
        "logEvents": [
            {
                "id": "event1",
                "timestamp": 1678886400000,
                "message": "Standard log message 1"
            },
            {
                "id": "event2",
                "timestamp": 1678886401000,
                "message": mock_log_message_content
            },
            {
                "id": "event3",
                "timestamp": 1678886402000,
                "message": "Another standard log message"
            }
        ]
    }


@pytest.fixture(scope="module")
def mock_logs_event(mock_decoded_logs_data):
    """Creates a mock CloudWatch Logs event, gzipped and base64 encoded."""
    logs_data_json_str = json.dumps(mock_decoded_logs_data)
    logs_data_bytes = logs_data_json_str.encode('utf-8')
    compressed_data = gzip.compress(logs_data_bytes)
    encoded_data_str = base64.b64encode(compressed_data).decode('utf-8')
    return {
        "awslogs": {
            "data": encoded_data_str
        }
    }


@pytest.fixture
def mock_sns(mocker):
    """Set up mock SNS client."""
    mock_client = mocker.patch(f"{MODULE_PATH}.boto3.client")
    mock_instance = mock_client.return_value
    return mock_client

def test_decode_logs_event_success(mock_logs_event, mock_decoded_logs_data):
    """Test successful decoding of a CloudWatch Logs event."""
    decoded_data = decode_logs_event(mock_logs_event)
    assert decoded_data == mock_decoded_logs_data

def test_process_support_log_calls_send_alert(mocker, mock_log_message_content, mock_log_stream_name):
    """Test that process_support_log correctly parses the message and calls send_alert_notification."""
    mock_send_alert = mocker.patch(f"{MODULE_PATH}.send_alert_notification")
    
    process_support_log(mock_log_message_content, mock_log_stream_name)

    expected_alert_title = "Critical System Failure "
    expected_alert_body = f"Traceback: Error X occurred at Y. Details: Z.\nLog Stream: {mock_log_stream_name}"
    
    mock_send_alert.assert_called_once_with(expected_alert_title, expected_alert_body)


def test_process_support_log_malformed_no_content_tag(mocker, mock_log_stream_name):
    """Test with a malformed log message missing the [CONTENT] tag."""
    mock_send_alert = mocker.patch(f"{MODULE_PATH}.send_alert_notification")
    malformed_log_message = "[SUPPORT ALERT] Only a title, no content tag."
    
    with pytest.raises(ValueError):
        process_support_log(malformed_log_message, mock_log_stream_name)
    mock_send_alert.assert_not_called()


def test_send_alert_notification_publishes_to_sns(mock_sns, caplog):
    """Test that send_alert_notification publishes a message to the correct SNS topic."""
    caplog.set_level(logging.INFO)
    
    alert_title = "Test Alert Title"
    alert_body = "This is the body of the test alert.\nLog Stream: test/stream"
    expected_topic_arn = "arn:aws:sns:us-east-1:123456789012:TestReportingTopic"
    expected_subject = f'CRM Shared Layer Alerts: DealerPeak - {alert_title}'

    send_alert_notification(alert_title, alert_body)

    mock_sns.assert_called_once_with('sns')
    mock_sns.return_value.publish.assert_called_once_with(
        TopicArn=expected_topic_arn,
        Message=alert_body,
        Subject=expected_subject,
        MessageStructure='string'
    )


def test_lambda_handler_processes_support_alerts(mocker, mock_logs_event, mock_decoded_logs_data, mock_log_message_content, mock_log_stream_name, caplog):
    """Test lambda_handler successfully decodes and processes logs containing support alerts."""
    caplog.set_level(logging.INFO)
    mock_decode_event = mocker.patch(f"{MODULE_PATH}.decode_logs_event", return_value=mock_decoded_logs_data)
    mock_process_log = mocker.patch(f"{MODULE_PATH}.process_support_log")

    lambda_handler(mock_logs_event, mocker.MagicMock())

    mock_decode_event.assert_called_once_with(mock_logs_event)
    mock_process_log.assert_called_once_with(mock_log_message_content, mock_log_stream_name)
    
    assert f"Event: {mock_logs_event}" in caplog.text
    assert f"Decoded Event: {mock_decoded_logs_data}" in caplog.text