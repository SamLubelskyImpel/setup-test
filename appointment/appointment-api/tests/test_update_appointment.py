"""Tests for the update_appointment module.

AWS_PROFILE=unified-test pytest test_update_appointment.py
Requires VPN access to database.
"""
import os
import sys
from json import dumps, loads
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

os.environ["ENVIRONMENT"] = "test"

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
sys.path.append(app_dir)

layer_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"../layers/appt_orm/python/lib/python3.9/site-packages"))
sys.path.insert(0, layer_path)

from appt_orm.session_config import DBSession
from appt_orm.models.consumer import Consumer
from appt_orm.models.vehicle import Vehicle
from appt_orm.models.appointment import Appointment

import update_appointment
from utils import format_timestamp

mock_response_payload_1 = {
    "statusCode": 204,
    "body": dumps({
        "message": "success"
    })
}

mock_response_payload_2 = {
    "statusCode": 500,
    "body": dumps({
        'error': {
            'code': 'V002',
            'message': 'XTime responded with an error: x-001009 Timeslot is not available'
        }
    })
}

mock_response_2 = {
    "statusCode": 500,
    "body": dumps({
        "error": {
            "code": "I002",
            "message": "Unexpected response from vendor integration. Please contact Impel support."
        }
    })
}

mock_lambda_response_1 = MagicMock()
mock_lambda_response_1.read.return_value.decode.return_value = dumps(mock_response_payload_1)

mock_lambda_response_2 = MagicMock()
mock_lambda_response_2.read.return_value.decode.return_value = dumps(mock_response_payload_2)

mock_boto3_client = MagicMock()
mock_boto3_client.invoke.side_effect = [
    {"Payload": mock_lambda_response_1},
    {"Payload": mock_lambda_response_2},
]

@patch('boto3.client', return_value=mock_boto3_client)
def test_update_appointment(mock_boto3_client):

    timeslot = str(datetime.now(tz=None) + timedelta(days=1))
    test_event = {
        "queryStringParameters": {
            "dealer_integration_partner_id": 1,
            "appointment_id": 155
        },
        "httpMethod": "PUT",
        "path": "/test/path",
        "headers": {},
        "body": dumps({
            "timeslot": "2025-03-30T14:00:00",
        }),
        "requestContext": {
            "authorizer": {
                "request_product": "SERVICE_AI"
            },
        }
    }

    # Successful response
    test_context = MagicMock()
    response = update_appointment.lambda_handler(test_event, test_context)
    print(response)
    assert response['statusCode'] == 204
    response_copy = loads(response['body'])

    test_event_body = loads(test_event['body'])
    test_event_params = test_event['queryStringParameters']

    with DBSession() as session:
        appointment_info = session.query(
            Appointment
        ).filter(
            Appointment.id == test_event_params["appointment_id"]
        ).first()

    # try:
    #     print(format_timestamp(appointment_info.timeslot_ts.isoformat(), 'US/Pacific'))
    #     print(format_timestamp(test_event_body.get("timeslot"), 'US/Pacific'))
    #     assert appointment_info.timeslot_ts.isoformat() == format_timestamp(test_event_body.get("timeslot"), 'US/Pacific')

    # except Exception as e:
    #     print(f"Assertion error: {e}")

    # Error response from integration
    test_context_2 = MagicMock()
    response_2 = update_appointment.lambda_handler(test_event, test_context_2)
    assert response_2['statusCode'] == mock_response_2['statusCode']
    response_copy_2 = {k: v for k, v in loads(response_2['body']).items() if k != 'request_id'}
    print(response_copy_2)
    print(mock_response_payload_2['body'])
    assert response_copy_2 == loads(mock_response_payload_2['body'])

if __name__ == '__main__':
    test_update_appointment()
