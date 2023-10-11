"""CRM Base class."""


class BaseCrm:
    """CRM Base class."""

    name = "BASE"
    activity = None

    def handle_activity(self, activity: dict):
        return {
            "statusCode": 404
        }

    def get_lead_status(self):
        return {
            "statusCode": 404
        }

    def get_lead_salesperson(self):
        return {
            "statusCode": 404
        }
