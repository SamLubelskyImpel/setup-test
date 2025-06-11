import os
import sys
from json import dumps
from unittest.mock import patch, MagicMock
import unittest

# Set environment variables for the tests
os.environ["SECRET_KEY"] = "ESKIMO"
os.environ["ENVIRONMENT"] = "test"

# Add application directory to system path
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
sys.path.append(app_dir)

from api_wrappers import EskimoApiWrapper

class TestEskimoApiWrapper(unittest.TestCase):

    @patch('api_wrappers.secret_client')
    @patch('requests.request')
    def test_insert_note_success(self, mock_request, mock_secret_client):
        """Test inserting a note successfully."""
        # Mock Secrets Manager response
        mock_secret_client.get_secret_value.return_value = {
            "SecretString": dumps({
                "ESKIMO": dumps({  # Mock it as a JSON string, just like in production
                    "API_URL": "https://mock-api.com",
                    "API_PASSWORD": "mock_token"
                })
            })
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = 'Success'
        mock_request.return_value = mock_response

        activity = {
            "crm_dealer_id": "1234",
            "crm_consumer_id": "5678",
            "notes": "Sample note",
            "activity_type": "note"
        }

        wrapper = EskimoApiWrapper(activity=activity)
        response = wrapper.create_activity()

        self.assertEqual(response, 'Success')

        mock_request.assert_called_once_with(
            method="POST",
            url="https://mock-api.com/updatenote",
            json={
                "accountId": "1234",
                "customerId": "5678",
                "note": "Sample note"
            },
            headers={
                "Content-Type": "application/json"
            }
        )

    @patch('api_wrappers.secret_client')
    @patch('requests.request')
    def test_insert_appointment_success(self, mock_request, mock_secret_client):
        """Test inserting an appointment successfully."""
        # Mock Secrets Manager response
        mock_secret_client.get_secret_value.return_value = {
            "SecretString": dumps({
                "ESKIMO": dumps({  # Mock it as a JSON string
                    "API_URL": "https://mock-api.com",
                    "API_PASSWORD": "mock_token"
                })
            })
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = 'Success'
        mock_request.return_value = mock_response

        activity = {
            "crm_dealer_id": "1234",
            "crm_consumer_id": "5678",
            "activity_due_ts": "2024-09-30T12:00:00-04:00",
            "activity_type": "appointment"
        }

        wrapper = EskimoApiWrapper(activity=activity)
        response = wrapper.create_activity()

        self.assertEqual(response, 'Success')

        mock_request.assert_called_once_with(
            method="POST",
            url="https://mock-api.com/createappointment",
            json={
                "accountId": "1234",
                "customerId": "5678",
                "appointmentDate": "2024-09-30T12:00:00-04:00"
            },
            headers={
                "Content-Type": "application/json"
            }
        )

    @patch('api_wrappers.secret_client')
    @patch('requests.request')
    def test_invalid_activity_type(self, mock_request, mock_secret_client):
        """Test handling invalid activity type."""
        # Mock Secrets Manager response
        mock_secret_client.get_secret_value.return_value = {
            "SecretString": dumps({
                "ESKIMO": dumps({  # Mock it as a JSON string
                    "API_URL": "https://mock-api.com",
                    "API_PASSWORD": "mock_token"
                })
            })
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = 'Success'
        mock_request.return_value = mock_response

        activity = {
            "crm_dealer_id": "1234",
            "crm_consumer_id": "5678",
            "notes": "Sample note",
            "activity_type": "invalid"
        }

        wrapper = EskimoApiWrapper(activity=activity)
        response = wrapper.create_activity()

        self.assertIsNone(response)

        mock_request.assert_not_called()


if __name__ == '__main__':
    unittest.main()
