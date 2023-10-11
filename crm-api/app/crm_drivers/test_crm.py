"""Test CRM class."""

from os import environ
from json import dumps
import logging

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class TestCrm():
    """Test CRM class."""

    name = "TESTCRM"
    activity = None

    def __init__(self):
        """Load CRM secrets."""
        # secrets = loads(SM_CLIENT.get_secret_value(SecretId="CRM_APIS")[self.name])

        # self.api_url = secrets["API_URL"]
        # self.api_token = secrets["API_TOKEN"]
        pass

    def handle_activity(self, activity: dict, activity_type: str):
        """Handle CRM activity."""
        self.activity = activity

        if activity_type in ("note", "appointment", "outbound_call", "lead_status_change"):
            return {
                "statusCode": 200
            }

        logger.error(f"activity_type {activity_type} not supported by {self.name}")
        return {
            "statusCode": 500
        }

    def get_lead_salesperson(self):
        """Get lead salesperson from CRM."""
        return {
            "statusCode": 200,
            "body": dumps({
                "lead_salesperson": {
                    "id": 12345,
                    "first_name": "John",
                    "last_name": "Doe"
                }
            })
        }

    def get_lead_status(self):
        """Get lead status from CRM."""
        return {
            "statusCode": 200,
            "body": dumps({
                "lead_status": "ACTIVE"
            })
        }
