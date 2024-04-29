import unittest
from unittest.mock import patch
from appointment_handler import get_appt_time_slots, create_appointment, get_appointments

class TestLambdaFunctions(unittest.TestCase):
    @patch('appointment_handler.XTimeApiWrapper')
    def test_get_appt_time_slots(self, mock_wrapper):
        event = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "op_code": "10909807",
            "start_time": "2024-04-10T12:00:00",
            "end_time": "2024-04-17T12:00:00",
            "vin": "1FTHF35L0G0019158",
            "year": 2011,
            "make": "Ford",
            "model": "Explorer"
        }
        mock_wrapper.return_value.retrieve_appt_time_slots.return_value = {
            "success": True,
            "availableAppointments": [
                {
                    "appointmentDateTimeLocal": "2024-04-12T00:00:00",
                    "durationMinutes": 30
                }
            ]
        }

        response = get_appt_time_slots(event, None)

        self.assertEqual(response['statusCode'], 200)
        self.assertIsInstance(response['body'], dict)
        self.assertIsInstance(response['body'].get('available_timeslots'), list)
        if response['body'].get('available_timeslots'):
            for slot in response['body']['available_timeslots']:
                self.assertIsInstance(slot.get('timeslot'), str)
                self.assertIsInstance(slot.get('duration'), int)

    @patch('appointment_handler.XTimeApiWrapper')
    def test_create_appointment(self, mock_wrapper):
        event = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "op_code": "10909807",
            "timeslot": "2024-04-12T00:00:00",
            "duration": 30,
            "comment": "Customer will drop off the motor vehicle",
            "first_name": "John",
            "last_name": "Smith",
            "email_address": "john.smith@example.com",
            "phone_number": "123-456-7890",
            "vin": "1FTHF35L0G0019158",
            "year": 2011,
            "make": "Ford",
            "model": "Explorer"
        }
        mock_wrapper.return_value.create_appointments.return_value = {
            "success": True,
            "appointmentId": "123abc456"
        }

        response = create_appointment(event, None)

        self.assertEqual(response['statusCode'], 201)
        self.assertIsInstance(response['body'], dict)
        self.assertIsInstance(response['body'].get('appointment_id'), str)

    @patch('appointment_handler.XTimeApiWrapper')
    def test_get_appointments(self, mock_wrapper):
        event = {
            "request_id": "5aec6d02-239e-49d1-9c95-78cdb787df4f",
            "integration_dealer_id": "xts9010",
            "dealer_timezone": "America/New_York",
            "first_name": "John",
            "last_name": "Smith",
            "email_address": "john.smith@example.com",
            "phone_number": "123-456-7890",
            "vin": "1FTHF35L0G0019158"
        }
        mock_wrapper.return_value.retrieve_appointments.return_value = [
            {
                
            }
        ]

        response = get_appointments(event, None)

        self.assertEqual(response['statusCode'], 200)
        self.assertIsInstance(response['body'], dict)
        self.assertIsInstance(response['body'].get('appointments'), list)
        if response['body'].get('appointments'):
            for appointment in response['body']['appointments']:
                self.assertIsInstance(appointment.get('appointment_id'), str)
                self.assertIsInstance(appointment.get('timeslot'), str)
                self.assertIsInstance(appointment.get('timeslot_duration'), int)
                self.assertIsInstance(appointment.get('comment'), str)

if __name__ == '__main__':
    unittest.main()
