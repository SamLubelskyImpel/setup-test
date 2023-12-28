"""
These classes are designed to manage calls to the Dealerpeak/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import boto3
import logging
from os import environ
import requests
from uuid import uuid4
from json import loads
from datetime import datetime, timedelta
# from requests.auth import HTTPBasicAuth

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")

# finalize sender_name
REYREY_XML_TEMPLATE = """<?xml version="1.0" encoding="utf-8"?>
<rey_ImpelCRMUpdateSalesLead xmlns="http://www.starstandards.org/STAR" xsi:schemaLocation="http://www.starstandards.org/STAR schema.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ApplicationArea>
    <BODId>{bod_id}</BODId>
    <CreationDateTime>{created_date_time}</CreationDateTime>
    <Sender>
      <Component>"ImpelCRM"</Component>
      <Task>"USL"</Task>
      <TransType>"I"</TransType>
      <SenderName>"Impel AI"</SenderName>
    </Sender>
    <Destination>
      <DestinationNameCode>"RRCRM"</DestinationNameCode>
      <DealerNumber>{dealer_number}</DealerNumber>
      <StoreNumber>{store_number}</StoreNumber>
      <AreaNumber>{area_number}</AreaNumber>
    </Destination>
  </ApplicationArea>
  <Record>
    <Identifier>
      <ProspectId>{prospect_id}</ProspectId>
    </Identifier>
    {activity_schema}
    {appointment_schema}
    {note_schema}
  </Record>
</rey_ImpelCRMUpdateSalesLead>
"""
# finalize activity result
ACTIVITY_SCHEMA = """
<Activity>
    <ActivityCompletedOn>{completed_on}</ActivityCompletedOn>
    <ActivityName>{name}</ActivityName>
    <ContactMethod>"{contact_method}"</ContactMethod>
    <ActivityNote>{note}</ActivityNote>
    <ActivityResult>"?"</ActivityResult>
    <ActivityResultType>"Success"</ActivityResultType>
</Activity>
"""

APPOINTMENT_SCHEMA = """
<Appointment>
    <AppointmentScheduledDateTime>{scheduled_date_time}</AppointmentScheduledDateTime>
    <ActivityNote>{note}</ActivityNote>
    <AppointmentDateTime>"{date_time}"</AppointmentDateTime>
</Appointment>
"""

NOTE_SCHEMA = """
<Note>
    <NoteInsertedOn>{inserted_on}</NoteInsertedOn>
    <ProspectNote>{note}</ProspectNote>
    <NoteUpdatedOn>{updated_on}</NoteUpdatedOn>
</Note>
"""


class CRMApiError(Exception):
    pass


class CrmApiWrapper:
    """CRM API Wrapper."""
    def __init__(self) -> None:
        self.partner_id = CRM_API_SECRET_KEY
        self.api_key = self.get_secrets()

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
        )
        secret = loads(secret["SecretString"])[CRM_API_SECRET_KEY]
        secret_data = loads(secret)

        return secret_data["api_key"]

    def update_activity(self, activity_id, crm_activity_id):
        try:
            res = requests.put(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                json={
                    "crm_activity_id": crm_activity_id
                },
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                }
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")
            raise CRMApiError(f"Error occured calling CRM API: {e}")


class ReyreyApiWrapper:
    """ReyRey API Wrapper."""
    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = self.get_secrets()
        self.__activity = kwargs.get("activity")
        self.__store_number, self.__area_number, self.__dealer_number = self.__activity["crm_dealer_id"].split("_")
        self.__utc_offset = self.__activity.get("dealer_metadata").get("utc_offset")  # "+05:00"

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]

    def __call_api(self, payload):
        # response = requests.request(
        #     method=method,
        #     url=self.__url,
        #     json=payload,
        #     auth=HTTPBasicAuth(self.__username, self.__password)
        # )
        # response.raise_for_status()
        # logger.info(f"ReyRey response: {response}")
        # return response["task_id"]
        crm_activity_id = str(uuid4())
        return crm_activity_id

    def __compute_offset(self):
        # Extract the sign, hours, and minutes from the UTC offset string
        sign, offset_str = self.__utc_offset[0], self.__utc_offset[1:]
        hours, minutes = map(int, offset_str.split(':'))

        # Determine the sign of the offset and create a timedelta object
        offset_multiplier = 1 if sign == '+' else -1
        offset = timedelta(hours=offset_multiplier * hours, minutes=offset_multiplier * minutes)
        return offset

    def __insert_note(self):
        requested_date_time = datetime.strptime(self.__activity["activity_requested_ts"], "%Y-%m-%dT%H:%M:%SZ")
        time_offset = self.__compute_offset()
        created_date = (requested_date_time + time_offset).strftime("%Y-%m-%dT%H:%M:%S")
        request_id = uuid4()

        payload = REYREY_XML_TEMPLATE.format(
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema="",
            appointment_schema="",
            note_schema=NOTE_SCHEMA.format(
                inserted_on=created_date,
                note=self.__activity["notes"],
                updated_on=created_date,
            )
        )
        logger.info(f"XML payload to ReyRey: {payload}")
        return self.__call_api(payload)

    def __create_appointment(self):
        requested_date_time = datetime.strptime(self.__activity["activity_requested_ts"], "%Y-%m-%dT%H:%M:%SZ")
        due_date_time = datetime.strptime(self.__activity["activity_due_ts"], "%Y-%m-%dT%H:%M:%SZ")
        time_offset = self.__compute_offset()
        created_date = (requested_date_time + time_offset).strftime("%Y-%m-%dT%H:%M:%S")
        due_date = (due_date_time + time_offset).strftime("%Y-%m-%dT%H:%M:%S")
        request_id = uuid4()

        payload = REYREY_XML_TEMPLATE.format(
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema="",
            appointment_schema=APPOINTMENT_SCHEMA.format(
                scheduled_date_time=due_date,
                note=self.__activity["notes"],
                date_time=created_date,
            ),
            note_schema=""
        )
        logger.info(f"XML payload to ReyRey: {payload}")
        return self.__call_api(payload)

    def __create_activity(self):
        requested_date_time = datetime.strptime(self.__activity["activity_requested_ts"], "%Y-%m-%dT%H:%M:%SZ")
        time_offset = self.__compute_offset()
        created_date = (requested_date_time + time_offset).strftime("%Y-%m-%dT%H:%M:%S")
        request_id = uuid4()

        contact_method = self.__activity["contact_method"].capitalize()
        if contact_method not in ("Phone", "Email", "Text"):
            logger.error(f"ReyRey CRM doesn't support contact method: {contact_method}")
            raise

        payload = REYREY_XML_TEMPLATE.format(
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema=ACTIVITY_SCHEMA.format(
                completed_on=created_date,
                name=f'Contacted-{contact_method}',
                contact_method=contact_method,
                note=self.__activity["notes"],
            ),
            appointment_schema="",
            note_schema=""
        )
        logger.info(f"XML payload to ReyRey: {payload}")
        return self.__call_api(payload)

    def create_activity(self):
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        elif self.__activity["activity_type"] == "appointment":
            return self.__create_appointment()
        elif self.__activity["activity_type"] == "outbound_call":
            return self.__create_activity()
        else:
            logger.error(f"ReyRey CRM doesn't support activity type: {self.__activity['activity_type']}")
            return None
