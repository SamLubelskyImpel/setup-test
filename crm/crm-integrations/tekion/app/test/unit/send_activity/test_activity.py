from unittest.mock import Mock, patch
import pytest
from aws_lambda_powertools.utilities.batch import EventType
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from send_activity.handler import lambda_handler, record_handler
from send_activity.api_wrappers import TekionApiWrapper


@pytest.fixture
def sqs_record(send_activity_event):
    record = Mock(spec=SQSRecord)
    record.body = send_activity_event.as_json()
    record.message_id = "message_id"
    return record


@patch("send_activity.handler.TekionApiWrapper.create_activity")
@patch("send_activity.handler.logger")
def test_record_handler(mock_logger, mock_create_activity, sqs_record, send_activity_event):
    mock_create_activity.return_value = "activity_id"

    record_handler(record=sqs_record)

    mock_create_activity.assert_called_once()
    mock_logger.info.assert_any_call(f"Tekion response with Activity ID: activity_id")


@patch("send_activity.handler.BatchProcessor")
@patch("send_activity.handler.process_partial_response")
def test_lambda_handler(mock_process_response, mock_batch_processor, send_activity_event):
    event = {
        'Records': [
            {
                'messageId': '3c31e053-2ca3-40b2-bcfd-4d177c325231',
                'receiptHandle': 'AQEB8J9Z6...',
                'body': send_activity_event.as_json(),
            }
        ]
    }

    context = Mock(spec=LambdaContext)
    mock_batch_processor.return_value = Mock()

    lambda_handler(event=event, context=context)

    mock_batch_processor.assert_called_once_with(event_type=EventType.SQS)
    mock_process_response.assert_called_once()


def test_tekion_api_wrapper_insert_note(
    send_activity_event,
):
    tekion_wrapper = TekionApiWrapper(activity=send_activity_event)
    note = tekion_wrapper.create_activity()

    assert isinstance(note, str)


