import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import json

# Set environment variables for the tests
os.environ["SECRET_KEY"] = "TEST_KEY"
os.environ["ENVIRONMENT"] = "test"
os.environ["CRM_API_SECRET_KEY"] = "test"

# Patch `secret_client` before importing the application
with patch("app.api_wrappers.CrmApiWrapper.get_secrets") as mock_secret_client:
    mock_secret_client.get_secret_value.return_value = {
        "SecretString": json.dumps({"test":json.dumps({
            "API_PRIVATE_KEY": "mock_private_key",
            "API_PUBLIC_KEY": "mock_public_key",
            "API_URL": "https://mock-api.com"
        })})
    }
    # Import the application module
    from app.send_activity import lambda_handler
    from app.api_wrappers import DealersocketAUApiWrapper


class TestSendActivity(unittest.TestCase):
    @patch("app.api_wrappers.DealersocketAUApiWrapper.create_activity")
    def test_note_activity(self, mock_create_activity):
        """Test the lambda handler for a 'note' activity."""
        mock_create_activity.return_value = {
            "ActivityID": None,
            "ErrorCode": None,
        }

        sqs_event = {
            "Records": [
                {
                    "body": json.dumps(
                        {
                            "crm_dealer_id": "1234",
                            "crm_consumer_id": "5678",
                            "crm_lead_id": "9090",
                            "lead_id": "9090",
                            "notes": "Sample note",
                            "activity_type": "note",
                        }
                    )
                }
            ]
        }
        context = MagicMock()

        # Test lambda handler
        response = lambda_handler(sqs_event, context)
        self.assertIsNotNone(response, "Lambda handler did not return a response")
        mock_create_activity.assert_called_once()


if __name__ == "__main__":
    unittest.main()
