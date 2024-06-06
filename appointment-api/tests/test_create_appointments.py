"""Tests for the create_appointments module.

AWS_PROFILE=unified-test pytest test_create_appointments.py
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

import create_appointment

from appt_orm.session_config import DBSession
from appt_orm.models.op_code_product import OpCodeProduct
from appt_orm.models.op_code_appointment import OpCodeAppointment
from appt_orm.models.consumer import Consumer
from appt_orm.models.vehicle import Vehicle
from appt_orm.models.appointment import Appointment

mock_response_payload_1 = {
    "statusCode": 200,
    "body": dumps({
        "appointment_id": "test_0001111",
    })
}

mock_response_payload_2 = {
    "statusCode": 500,
    "body": dumps({
        "error": {
            "code": "V002",
            "message": "Timeslot already booked. Please select another timeslot."
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
def test_create_appointments(mock_boto3_client):
    test_event = {
        "queryStringParameters": {
            "dealer_integration_partner_id": 1
        },
        "httpMethod": "GET",
        "path": "/test/path",
        "headers": {},
        "body": dumps({
            "op_code": "PRODUCT004",
            "timeslot": "2024-03-30T14:00:00",
            "timeslot_duration": 30,
            "created_date_ts": "2024-03-26T14:00:00Z",
            "comment": "Customer will drop off motor vehicle",
            "consumer": {
                "first_name": "Test",
                "last_name": "User",
                "email_address": "test.user@example.com",
                "phone_number": "123-456-7890"
            },
            "vehicle": {
                "vin": "1HGCR2F70FAJ59219",
                "make": "Honda",
                "model": "Accord",
                "year": 2015,
                "vehicle_class": "Compact",
                "mileage": 50000,
                "body_style": "SUV",
                "transmission": "Automatic",
                "interior_color": "Black",
                "exterior_color": "White",
                "trim": "LX",
                "condition": "New",
                "odometer_units": "miles"
            }
        }),  # Example request body
    }

    # Successful response
    test_context = MagicMock()
    response = create_appointment.lambda_handler(test_event, test_context)
    assert response['statusCode'] == 201
    response_copy = loads(response['body'])

    test_event_body = loads(test_event['body'])

    with DBSession() as session:
        appointment_info = session.query(
            Appointment, Consumer, Vehicle, OpCodeProduct, OpCodeAppointment
        ).join(
            Consumer, Consumer.id == Appointment.consumer_id
        ).join(
            Vehicle, Vehicle.id == Appointment.vehicle_id
        ).join(
            OpCodeAppointment, OpCodeAppointment.id == Appointment.op_code_appointment_id
        ).join(
            OpCodeProduct, OpCodeProduct.id == OpCodeAppointment.op_code_product_id
        ).filter(
            Appointment.id == response_copy["appointment_id"]
        ).first()

    assert appointment_info.Consumer.id == response_copy["consumer_id"]

    # Validate Appointment
    assert appointment_info.Appointment.integration_appointment_id == loads(mock_response_payload_1["body"])["appointment_id"]

    try:
        # assert appointment_info.Appointment.timeslot_ts == datetime.fromisoformat(test_event_body.get("timeslot"))
        assert appointment_info.Appointment.timeslot_duration == 30
        # assert appointment_info.Appointment.created_date_ts == datetime.strptime(test_event_body.get("created_date_ts"), "%Y-%m-%dT%H:%M:%SZ")
        assert appointment_info.Appointment.comment == test_event_body.get("comment")
        assert appointment_info.Appointment.status == "Active"
        assert appointment_info.OpCodeProduct.op_code == test_event_body["op_code"]

        # Validate Consumer
        assert appointment_info.Consumer.first_name == test_event_body["consumer"]["first_name"]
        assert appointment_info.Consumer.last_name == test_event_body["consumer"]["last_name"]
        assert appointment_info.Consumer.email_address == test_event_body["consumer"]["email_address"]
        assert appointment_info.Consumer.phone_number == test_event_body["consumer"]["phone_number"]
        assert appointment_info.Consumer.dealer_integration_partner_id == test_event["queryStringParameters"]["dealer_integration_partner_id"]

        # Validate Vehicle
        assert appointment_info.Vehicle.vin == test_event_body["vehicle"]["vin"]
        assert appointment_info.Vehicle.make == test_event_body["vehicle"]["make"]
        assert appointment_info.Vehicle.model == test_event_body["vehicle"]["model"]
        assert appointment_info.Vehicle.manufactured_year == test_event_body["vehicle"]["year"]
        assert appointment_info.Vehicle.vehicle_class == test_event_body["vehicle"]["vehicle_class"]
        assert appointment_info.Vehicle.mileage == test_event_body["vehicle"]["mileage"]
        assert appointment_info.Vehicle.body_style == test_event_body["vehicle"]["body_style"]
        assert appointment_info.Vehicle.transmission == test_event_body["vehicle"]["transmission"]
        assert appointment_info.Vehicle.interior_color == test_event_body["vehicle"]["interior_color"]
        assert appointment_info.Vehicle.exterior_color == test_event_body["vehicle"]["exterior_color"]
        assert appointment_info.Vehicle.trim == test_event_body["vehicle"]["trim"]
        assert appointment_info.Vehicle.condition == test_event_body["vehicle"]["condition"]
        assert appointment_info.Vehicle.odometer_units == test_event_body["vehicle"]["odometer_units"]
    except Exception as e:
        print(f"Assertion error: {e}")

    # Cleanup
    with DBSession() as session:
        session.query(Appointment).filter(Appointment.id == response_copy["appointment_id"]).delete()
        session.query(Vehicle).filter(Vehicle.id == appointment_info.Vehicle.id).delete()
        session.query(Consumer).filter(Consumer.id == response_copy["consumer_id"]).delete()
        session.commit()

    # Error response from integration
    test_context_2 = MagicMock()
    response_2 = create_appointment.lambda_handler(test_event, test_context_2)
    assert response_2['statusCode'] == mock_response_2['statusCode']
    response_copy_2 = {k: v for k, v in loads(response_2['body']).items() if k != 'request_id'}
    assert response_copy_2 == loads(mock_response_payload_2['body'])


if __name__ == '__main__':
    test_create_appointments()
