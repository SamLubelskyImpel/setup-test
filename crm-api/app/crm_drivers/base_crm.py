"""CRM Base class."""


class BaseCrm:
    """CRM Base class."""

    name = "BASE"
    activity = None

    def handle_activity(self, activity_id):
        pass

    def add_note(self):
        pass

    def appointment(self):
        pass

    def outbound_call(self):
        pass

    def set_lead_status(self):
        pass

    def get_lead_status(self):
        pass

    def get_lead_salesperson(self):
        pass
