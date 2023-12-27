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


xml_template = """<?xml version="1.0" encoding="utf-8"?>
<rey_ImpelCRMUpdateSalesLead xmlns="http://www.starstandards.org/STAR" xsi:schemaLocation="http://www.starstandards.org/STAR schema.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ApplicationArea>
    <BODId>{bod_id}</BODId>
    <CreationDateTime>{created_date_time}</CreationDateTime>
    <Sender>
      <Component>"ImpelCRM"</Component>
      <Task>"USL"</Task>
      <TransType>"I"</TransType>
      <SenderName>{sender_name}</SenderName>
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

activity_schema = """
<Activity>
    <ActivityCompletedOn>{completed_on}</ActivityCompletedOn>
    <ActivityName>{name}</ActivityName>
    <ContactMethod>{contact_method}</ContactMethod>
    <ActivityNote>{note}</ActivityNote>
    <ActivityResult>{result}</ActivityResult>
    <ActivityResultType>"Success"</ActivityResultType>
</Activity>
"""

appointment_schema = """
<Appointment>
    <AppointmentScheduledDateTime>{scheduled_date_time}</AppointmentScheduledDateTime>
    <ActivityNote>{note}</ActivityNote>
    <AppointmentDateTime>{date_time}</AppointmentDateTime>
</Appointment>
"""

note_schema = """
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
        self.__utc_offset = loads(self.__activity.get("dealer_metadata")).get("utc_offset")  # "+05:00"

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
        # logger.info(f"ReyRey response: {response}")
        # return response
        pass

    def __insert_note(self):
        requested_date_time = datetime.strptime(self.__activity["activity_requested_ts"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_date = (requested_date_time + timedelta(hours=self.__utc_offset)).strftime("%Y-%m-%dT%H:%M:%S")
        request_id = uuid4()

        xml_template.format(
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema="",
            appointment_schema="",
            note_schema=note_schema.format(
                inserted_on=created_date,
                note=self.__activity["notes"],
                updated_on=created_date,
            )
        )
        logger.info(f"XML payload to ReyRey: {xml_template}")
        self.__call_api(xml_template)
        pass

    def __create_appointment(self):
        requested_date_time = datetime.strptime(self.__activity["activity_requested_ts"], "%Y-%m-%dT%H:%M:%S.%fZ")
        due_date_time = datetime.strptime(self.__activity["activity_due_ts"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_date = (requested_date_time + timedelta(hours=self.__utc_offset)).strftime("%Y-%m-%dT%H:%M:%S")
        due_date = (due_date_time + timedelta(hours=self.__utc_offset)).strftime("%Y-%m-%dT%H:%M:%S")
        request_id = uuid4()

        xml_template.format(
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema="",
            appointment_schema=appointment_schema.format(
                scheduled_date_time=due_date,
                note=self.__activity["notes"],
                date_time=created_date,
            ),
            note_schema=""
        )
        logger.info(f"XML payload to ReyRey: {xml_template}")
        self.__call_api(xml_template)
        pass

    def __create_activity(self):
        requested_date_time = datetime.strptime(self.__activity["activity_requested_ts"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_date = (requested_date_time + timedelta(hours=self.__utc_offset)).strftime("%Y-%m-%dT%H:%M:%S")
        request_id = uuid4()

        xml_template.format(
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema=activity_schema.format(
                completed_on=created_date,
                name=self.__activity["activity_type"],
                contact_method=self.__activity["contact_method"],
                note=self.__activity["notes"],
            ),
            appointment_schema="",
            note_schema=""
        )
        logger.info(f"XML payload to ReyRey: {xml_template}")
        self.__call_api(xml_template)
        pass

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
