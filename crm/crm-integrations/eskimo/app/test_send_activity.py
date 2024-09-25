import os
import sys
from json import dumps, loads
from unittest.mock import patch, MagicMock
import unittest

# Simulate environment variables for testing
os.environ["SECRET_KEY"] = "ESKIMO"  # Ensure SECRET_KEY is set
os.environ["ENVIRONMENT"] = "test"

# Add app directory to the path
app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
sys.path.append(app_dir)

# Import the module to test
from api_wrappers import EskimoApiWrapper

class TestEskimoApiWrapper(unittest.TestCase):  # Class name starts with "Test"

    @patch('api_wrappers.secret_client')  # Mocking the secret client here
    @patch('requests.request')
    def test_insert_note_success(self, mock_request, mock_secret_client):  # Method starts with "test_"
        """Test inserting a note successfully."""
        # Mock Secrets Manager response
        mock_secret_client.get_secret_value.return_value = {
            "SecretString": dumps({
                "ESKIMO": {
                    "API_URL": "https://mock-api.com",
                    "API_PASSWORD": "mock_token"
                }
            })
        }
    
        # Mock the API response
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

        # Initialize Eskimo API wrapper
        wrapper = EskimoApiWrapper(activity=activity)
        response = wrapper.create_activity()

        # Assert the expected response
        self.assertEqual(response, 'Success')

        # Verify the API call
        mock_request.assert_called_once_with(
            method="POST",
            url="https://mock-api.com",
            json={
                "accountId": "1234",
                "customerId": "5678",
                "note": "Sample note"
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer mock_token"
            }
        )

    @patch('api_wrappers.secret_client')  # Mocking the secret client here
    @patch('requests.request')
    def test_insert_appointment_success(self, mock_request, mock_secret_client):  # Method starts with "test_"
        """Test inserting an appointment successfully."""
        # Mock Secrets Manager response
        mock_secret_client.get_secret_value.return_value = {
            "SecretString": dumps({
                "ESKIMO": {
                    "API_URL": "https://mock-api.com",
                    "API_PASSWORD": "mock_token"
                }
            })
        }

        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = 'Success'
        mock_request.return_value = mock_response

        activity = {
            "crm_dealer_id": "1234",
            "crm_consumer_id": "5678",
            "activity_due_ts": "2024-09-30T12:00:00Z",
            "activity_type": "appointment"
        }

        # Initialize Eskimo API wrapper
        wrapper = EskimoApiWrapper(activity=activity)
        response = wrapper.create_activity()

        # Assert the expected response
        self.assertEqual(response, 'Success')

        # Verify the API call
        mock_request.assert_called_once_with(
            method="POST",
            url="https://mock-api.com",
            json={
                "accountId": "1234",
                "customerId": "5678",
                "appointmentDate": "2024-09-30T12:00:00Z"
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer mock_token"
            }
        )

    @patch('api_wrappers.secret_client')  # Mocking the secret client here
    @patch('requests.request')
    def test_invalid_activity_type(self, mock_request, mock_secret_client):  # Method starts with "test_"
        """Test handling invalid activity type."""
        # Mock Secrets Manager response
        mock_secret_client.get_secret_value.return_value = {
            "SecretString": dumps({
                "ESKIMO": {
                    "API_URL": "https://mock-api.com",
                    "API_PASSWORD": "mock_token"
                }
            })
        }

        # Mock the API response
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

        # Initialize Eskimo API wrapper
        wrapper = EskimoApiWrapper(activity=activity)
        response = wrapper.create_activity()

        # Assert that invalid activity returns None
        self.assertIsNone(response)


if __name__ == '__main__':
    unittest.main()
