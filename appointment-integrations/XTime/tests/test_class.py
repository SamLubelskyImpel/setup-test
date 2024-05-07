"""
Tests for the create_appointments api.
AWS_PROFILE=unified-test pytest
"""
import os
import sys
import unittest
from json import loads

os.environ["ENVIRONMENT"] = "test"

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
sys.path.append(app_dir)

from appointment_handler import (
    get_appt_time_slots,
    create_appointment,
    get_appointments,
)

class TestLambdaFunctions(unittest.TestCase):

    def test_create_appointment(self):
        event_success = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "op_code": "9648760",
            "timeslot": "2024-05-21T00:00:00",
            "duration": 15,
            "comment": "Customer will drop off the motor vehicle",
            "first_name": "John",
            "last_name": "Smith",
            "email_address": "john.smith@example.com",
            "phone_number": "123-456-7890",
            "vin": "3MZBN1L31JM173950",
            "year": 2011,
            "make": "Ford",
            "model": "Explorer"
        }

        response = create_appointment(event_success, None)

        self.assertEqual(response["statusCode"], 201)
        body_json = loads(response["body"])
        self.assertIsInstance(body_json, dict)
        self.assertIsInstance(body_json.get("appointment_id"), str)

        event_no_vin = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "op_code": "9648760",
            "timeslot": "2024-05-21T00:00:00",
            "duration": 15,
            "comment": "Customer will drop off the motor vehicle",
            "first_name": "John",
            "last_name": "Smith",
            "email_address": "john.smith@example.com",
            "phone_number": "123-456-7890",
            "vin": None,
            "year": 2011,
            "make": "Ford",
            "model": "Explorer"
        }

        response = create_appointment(event_no_vin, None)

        self.assertEqual(response["statusCode"], 500)
        body_json = loads(response["body"])
        self.assertIsInstance(body_json, dict)
        self.assertIsInstance(body_json['error'], dict)
        self.assertIsInstance(body_json['error']['message'], str)


    def test_time_slots(self):
        event_success = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "op_code": "8990394",
            "start_time": "2024-05-20T12:00:00",
            "end_time": "2024-05-22T12:00:00",
            # "vin": "3MZBN1L31JM173950",
            "year": 2011,
            "make": "Ford",
            "model": "Explorer"
        }

        response = get_appt_time_slots(event_success, None)

        self.assertEqual(response["statusCode"], 200)
        body_json = loads(response["body"])
        self.assertIsInstance(body_json, dict)
        self.assertIsInstance(body_json.get("available_timeslots"), list)

        event_no_vin = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "op_code": "9648748",
            "start_time": "2024-05-20T12:00:00",
            "end_time": "2024-05-22T12:00:00",
        }

        response = get_appt_time_slots(event_no_vin, None)

        self.assertEqual(response["statusCode"], 500)
        

    def test_get_appointments(self):
        event_success = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "first_name": "John",
            "last_name": "Smith",
            "email_address": "john.smith@example.com",
            "phone_number": "123-456-7890",
            "vin": "3MZBN1L31JM173950",
            "year": 2011,
            "make": "Ford",
            "model": "Explorer"
        }

        response = get_appointments(event_success, None)

        self.assertEqual(response["statusCode"], 200)
        body_json = loads(response["body"])
        self.assertIsInstance(body_json, dict)
        self.assertIsInstance(body_json.get("appointments"), list)

        event_no_vin = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "first_name": "John",
            "last_name": "Smith",
            "email_address": "john.smith@example.com",
            "phone_number": "123-456-7890",
        }

        response = get_appointments(event_no_vin, None)

        self.assertEqual(response["statusCode"], 500)
        body_json = loads(response["body"])
        self.assertIsInstance(body_json, dict)
        self.assertIsInstance(body_json['error'], dict)
        self.assertIsInstance(body_json['error']['message'], str)