"""Tests for the retrieve_timeslots module.

AWS_PROFILE=unified-test pytest test_timeslots.py
"""
import os
import sys
from json import dumps, loads
from unittest.mock import patch, MagicMock

os.environ["ENVIRONMENT"] = "test"

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
sys.path.append(app_dir)

layer_path = os.path.abspath("../layers/appt_orm/python/lib/python3.9/site-packages")
sys.path.insert(0, layer_path)

import retrieve_timeslots

mock_response_payload_1 = {
    "statusCode": 200,
    "body": dumps({
        "available_timeslots": [
            {
                "timeslot": "2024-04-17T12:00:00",
                "duration": 30
            },
            {
                "timeslot": "2024-04-17T14:00:00",
                "duration": 30
            }
        ],
    })
}

mock_response_1 = {
    "statusCode": 200,
    "body": dumps({
        "timeslots": [
            {
                "timeslot": "2024-04-17T12:00:00",
                "duration": 30
            },
            {
                "timeslot": "2024-04-17T14:00:00",
                "duration": 30
            }
        ],
    })
}

mock_response_payload_2 = {
    "statusCode": 500,
    "body": dumps({
        "error": {
            "code": "V001",
            "message": "XTime integration encountered an error"
        }
    })
}

mock_response_payload_3 = {
    "statusCode": 502,
    "body": dumps({
        "message": "Service unavailable"
    })
}

mock_response_3 = {
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

mock_lambda_response_3 = MagicMock()
mock_lambda_response_3.read.return_value.decode.return_value = dumps(mock_response_payload_3)

mock_boto3_client = MagicMock()
mock_boto3_client.invoke.side_effect = [
    {"Payload": mock_lambda_response_1},
    {"Payload": mock_lambda_response_2},
    {"Payload": mock_lambda_response_3}
]

@patch('boto3.client', return_value=mock_boto3_client)
def test_timeslots(mock_boto3_client):
    test_event = {
        "queryStringParameters": {
            "dealer_integration_partner_id": "1",
            "end_time": "2024-03-30T14:00:00",
            "make": "Ford",
            "model": "F-150",
            "op_code": "PRODUCT001",
            "start_time": "2024-03-25T14:00:00",
            "vin": "1HGBH41JXMN109186",
            "year": "2025"
        },
        "httpMethod": "GET",  # Example HTTP method
        "path": "/test/path",  # Example resource path
        "headers": {},  # Example headers
        "body": None,  # Example request body
    }

    # Successful response
    test_context = MagicMock()
    response = retrieve_timeslots.lambda_handler(test_event, test_context)
    assert response['statusCode'] == mock_response_1['statusCode']
    response_copy = {k: v for k, v in loads(response['body']).items() if k != 'request_id'}
    assert response_copy == loads(mock_response_1['body'])

    # Error response from integration
    test_context_2 = MagicMock()
    response_2 = retrieve_timeslots.lambda_handler(test_event, test_context_2)
    assert response_2['statusCode'] == mock_response_payload_2['statusCode']
    response_copy_2 = {k: v for k, v in loads(response_2['body']).items() if k != 'request_id'}
    assert response_copy_2 == loads(mock_response_payload_2['body'])

    # Unexpected response from integration
    test_context_3 = MagicMock()
    response_3 = retrieve_timeslots.lambda_handler(test_event, test_context_3)
    assert response_3['statusCode'] == mock_response_3['statusCode']
    response_copy_3 = {k: v for k, v in loads(response_3['body']).items() if k != 'request_id'}
    assert response_copy_3 == loads(mock_response_3['body'])


if __name__ == '__main__':
    test_timeslots()
