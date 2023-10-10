"""Test CRM class."""

from json import dumps
from crm_orm.models.activity import Activity
from crm_orm.models.activity_type import ActivityType
from crm_orm.session_config import DBSession
from base_crm import BaseCrm


class TestCrm(BaseCrm):
    """Test CRM class."""

    name = "TESTCRM"
    activity = None

    def __init__(self):
        # secrets = loads(SM_CLIENT.get_secret_value(SecretId="CRM_APIS")[self.name])

        # self.api_url = secrets["API_URL"]
        # self.api_token = secrets["API_TOKEN"]
        pass

    def handle_activity(self, activity_id):
        with DBSession() as session:
            self.activity = session.query(
                Activity
            ).filter(
                Activity.id == activity_id
            ).first()

            activity_type = session.query(
                ActivityType.type
            ).filter(
                ActivityType.id == self.activity.activity_type_id
            ).first()

        activity_type = activity_type.lower()
        if activity_type == "note":
            return self.add_note()
        elif activity_type == "appointment":
            return self.appointment()
        elif activity_type == "outbound_call":
            return self.outbound_call()
        elif activity_type == "lead_status_change":
            return self.set_lead_status()

        return {
            "statusCode": 500,
            "message": f"activity_type {activity_type} not supported by {self.name}"
        }

    def add_note(self):
        return {
            "statusCode": 200
        }

    def appointment(self):
        return {
            "statusCode": 200
        }

    def outbound_call(self):
        return {
            "statusCode": 200
        }

    def set_lead_status(self):
        return {
            "statusCode": 200
        }

    def get_lead_salesperson(self):
        return {
            "statusCode": 200,
            "body": dumps({
                "salesperson_id": 12345,
                "first_name": "John",
                "last_name": "Doe"
            })
        }

    def get_lead_status(self):
        return {
            "statusCode": 200,
            "body": dumps({
                "lead_status": "ACTIVE"
            })
        }
