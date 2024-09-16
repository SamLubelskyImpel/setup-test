"""Tests for the retrieve_appointments module.

AWS_PROFILE=unified-test pytest test_retrieve_appointments.py
Requires VPN access to database.
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

import retrieve_appointments

mock_response_payload_1 = {
    "statusCode": 200,
    "body": dumps({
        "appointments": [{
            "appointment_id": "4121cb9c-5182-4667-9af4-066210d5a424",
            "vin": "2HGBH41JXMN109185",
            "timeslot": "2024-04-26T14:00:00",
            "timeslot_duration": 30,
            "comment": "This is a comment",
            "first_name": "Jane",
            "last_name": "Smith",
            "email_address": "jane.smith@example.com",
            "phone_number": "123-456-7891",
            "services": [
                {
                    "op_code": "VENDOR004",
                    "service_name": "Oil Change",
                }
            ]
        }]
    })
}

mock_response_1 = {
    'statusCode': 200,
    'body': dumps({
        "appointments": [{
            "id": 3,
            "op_code": "PRODUCT003",
            "timeslot": "2024-03-30T11:00:00",
            "timeslot_duration": 30,
            "created_date_ts": "2024-03-26T14:00:00+00:00",
            "comment": "Customer will drop off motor vehicle",
            "status": "Active",
            "consumer": {
                "first_name": "Jane",
                "last_name": "Smith",
                "email_address": "jane.smith@example.com",
                "phone_number": "123-456-7891"
            },
            "vehicle": {
                "vin": "2HGBH41JXMN109185",
                "make": "Ford",
                "model": "F-150",
                "year": 2024,
                "vehicle_class": "Compact",
                "mileage": 50000,
                "body_style": "SUV",
                "transmission": "Manual",
                "interior_color": "Black",
                "exterior_color": "White",
                "trim": None,
                "condition": "New",
                "odometer_units": "miles"
                }
            }]
    })
}

mock_response_payload_2 = {
    "statusCode": 500,
    "body": dumps({
        "error": {
            "code": "V002",
            "message": "Unexpected response from XTime integration."
        }
    })
}

mock_response_payload_3 = {
    "statusCode": 200,
    "body": dumps({
        "appointments": [{
            "appointment_id": "4121cb9c-5182-4667-9af4-066210d5a424",
            "vin": "2HGBH41JXMN109185",
            "timeslot": "2024-04-26T14:00:00",
            "timeslot_duration": 30,
            "comment": "This is a comment",
            "first_name": "Jane",
            "last_name": "Smith",
            "email_address": "jane.smith@example.com",
            "phone_number": "123-456-7891",
            "op_code": "VENDOR004"
        }]
    })
}

mock_response_3 = {
    "statusCode": 200,
    "body": dumps({
        "appointments": [{
            "id": 3,
            "op_code": "PRODUCT003",
            "timeslot": "2024-03-30T11:00:00",
            "timeslot_duration": 30,
            "created_date_ts": "2024-03-26T14:00:00+00:00",
            "comment": "Customer will drop off motor vehicle",
            "status": "Active",
            "consumer": {
                "first_name": "Jane",
                "last_name": "Smith",
                "email_address": "jane.smith@example.com",
                "phone_number": "123-456-7891"
            },
            "vehicle": {
                "vin": "2HGBH41JXMN109185",
                "make": "Ford",
                "model": "F-150",
                "year": 2024,
                "vehicle_class": "Compact",
                "mileage": 50000,
                "body_style": "SUV",
                "transmission": "Manual",
                "interior_color": "Black",
                "exterior_color": "White",
                "trim": None,
                "condition": "New",
                "odometer_units": "miles"
            }
        }, {
            "id": 4,
            "op_code": "PRODUCT003",
            "timeslot": "2024-03-30T11:00:00",
            "timeslot_duration": 30,
            "created_date_ts": "2024-03-26T14:00:00+00:00",
            "comment": "Customer will drop off motor vehicle",
            "status": "Closed",
            "consumer": {
                "first_name": "Jane",
                "last_name": "Smith",
                "email_address": "jane.smith@example.com",
                "phone_number": "123-456-7891"
            },
            "vehicle": {
                "vin": "2HGBH41JXMN109185",
                "make": "Ford",
                "model": "F-150",
                "year": 2024,
                "vehicle_class": "Compact",
                "mileage": 50000,
                "body_style": "SUV",
                "transmission": "Manual",
                "interior_color": "Black",
                "exterior_color": "White",
                "trim": "GT",
                "condition": "New",
                "odometer_units": "miles"
            }
        }, {
            "id": 5,
            "op_code": "PRODUCT002",
            "timeslot": "2024-04-26T11:00:00",
            "timeslot_duration": 30,
            "created_date_ts": "2024-03-26T14:00:00+00:00",
            "comment": "Customer will drop off motor vehicle",
            "status": "Closed",
            "consumer": {
                "first_name": "Jane",
                "last_name": "Smith",
                "email_address": "jane.smith@example.com",
                "phone_number": "123-456-7891"
            },
            "vehicle": {
                "vin": "2HGBH41JXMN109185",
                "make": "Ford",
                "model": "F-150",
                "year": 2024,
                "vehicle_class": None,
                "mileage": None,
                "body_style": None,
                "transmission": None,
                "interior_color": None,
                "exterior_color": None,
                "trim": None,
                "condition": None,
                "odometer_units": None
            }
        }]
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


def remove_varying_fields(response_body: dict):
    return {
        k: v if not isinstance(v, list) else sorted([
            {
                k2: v2 if k2 != 'consumer' else {
                    k3: v3 for k3, v3 in v2.items() if k3 != 'id'
                }
                for k2, v2 in item.items()
            }
            for item in v
        ], key=lambda x: x['id'])
        for k, v in response_body.items() if k != 'request_id'
    }


@patch('boto3.client', return_value=mock_boto3_client)
def test_retrieve_appointments(mock_boto3_client):
    test_event = {
        "queryStringParameters": {
            "dealer_integration_partner_id": "1",
            "first_name": "Jane",
            "last_name": "Smith",
            "phone_number": "123-456-7891",
            "status": "Active",
            "vin": "2HGBH41JXMN109185"
        },
        "httpMethod": "GET",  # Example HTTP method
        "path": "/test/path",  # Example resource path
        "headers": {},  # Example headers
        "body": None,  # Example request body
    }

    # Successful response w/ Active status
    test_context = MagicMock()
    response = retrieve_appointments.lambda_handler(test_event, test_context)
    assert response['statusCode'] == mock_response_1['statusCode']
    response_copy = remove_varying_fields(loads(response['body']))
    assert response_copy == loads(mock_response_1['body'])

    # Error response from integration
    test_context_2 = MagicMock()
    response_2 = retrieve_appointments.lambda_handler(test_event, test_context_2)
    assert response_2['statusCode'] == mock_response_payload_2['statusCode']
    response_copy_2 = remove_varying_fields(loads(response_2['body']))
    assert response_copy_2 == loads(mock_response_payload_2['body'])

    # Successful response
    test_context_3 = MagicMock()
    test_event_3 = test_event.copy()
    test_event_3['queryStringParameters'].pop('status', None)
    response_3 = retrieve_appointments.lambda_handler(test_event_3, test_context_3)
    assert response_3['statusCode'] == mock_response_3['statusCode']
    response_copy_3 = remove_varying_fields(loads(response_3['body']))
    assert response_copy_3 == loads(mock_response_3['body'])


if __name__ == '__main__':
    test_retrieve_appointments()
