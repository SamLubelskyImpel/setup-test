"""
These classes are designed to manage calls to the ReyRey/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.

ReyRey API requires a SOAP XML payload to be sent to their API.
"""

import boto3
import logging
from os import environ
import requests
from uuid import uuid4
from json import loads
from datetime import datetime
import xmltodict
import pytz

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")
s3_client = boto3.client('s3')


REYREY_XML_TEMPLATE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <soap:Header>
        <wsse:Security>
            <wsse:UsernameToken>
                <wsse:Username>{auth_username}</wsse:Username>
                <wsse:Password>{auth_password}</wsse:Password>
            </wsse:UsernameToken>
        </wsse:Security>
    </soap:Header>
    <soap:Body>
        <rey_ImpelCRMUpdateSalesLead xmlns="http://www.starstandards.org/STAR">
            <ApplicationArea>
                <BODId>{bod_id}</BODId>
                <CreationDateTime>{created_date_time}</CreationDateTime>
                <Sender>
                    <Component>ImpelCRM</Component>
                    <Task>USL</Task>
                    <TransType>I</TransType>
                    <SenderName>Impel</SenderName>
                </Sender>
                <Destination>
                    <DestinationNameCode>RRCRM</DestinationNameCode>
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
    </soap:Body>
</soap:Envelope>
"""

ACTIVITY_SCHEMA = """
<Activity>
    <ActivityCompletedOn>{completed_on}</ActivityCompletedOn>
    <ActivityName>{name}</ActivityName>
    <ContactMethod>{contact_method}</ContactMethod>
    <ActivityNote>{note}</ActivityNote>
    <ActivityResult>Success</ActivityResult>
    <ActivityResultType>Success</ActivityResultType>
</Activity>
"""

APPOINTMENT_SCHEMA = """
<Appointment>
    <AppointmentScheduledDateTime>{scheduled_date_time}</AppointmentScheduledDateTime>
    <ActivityNote><![CDATA[{note}]]></ActivityNote>
    <AppointmentDateTime>{date_time}</AppointmentDateTime>
</Appointment>
"""

NOTE_SCHEMA = """
<Note>
    <NoteInsertedOn>{inserted_on}</NoteInsertedOn>
    <ProspectNote><![CDATA[{note}]]></ProspectNote>
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

    def get_activity(self, activity_id: int):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        response.raise_for_status()
        logger.info(f"CRM API -get_activity- responded with: {response.status_code}")

        if response.status_code != 200:
            raise Exception(f"Error getting activity {activity_id}: {response.text}")

        activity = response.json()
        if not activity:
            raise Exception(f"Activity not found for ID: {activity_id}")

        return activity

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

    def get_lead_status(self, lead_id):
        try:
            res = requests.get(
                url=f"https://{CRM_API_DOMAIN}/leads/{lead_id}/status",
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                }
            )
            res.raise_for_status()
            lead_status = res.json().get('lead_status')
            logger.info(f"Lead status: {lead_status}")
            return lead_status
        except Exception as e:
            raise Exception(f"Error occurred calling CRM API: {e}")

    def get_dealer_by_idp_dealer_id(self, idp_dealer_id: str):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/dealers/idp/{idp_dealer_id}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        response.raise_for_status()
        logger.info(f"CRM API -get_dealer_by_idp_dealer_id- responded with: {response.status_code}")

        if response.status_code != 200:
            raise Exception(f"Error getting dealer {idp_dealer_id}: {response.text}")

        dealer = response.json()
        if not dealer:
            raise Exception(f"Dealer not found for idp_dealer_id: {idp_dealer_id}")

        return dealer


class ReyreyApiWrapper:
    """ReyRey API Wrapper."""
    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = self.get_secrets()
        self.__activity = kwargs.get("activity")
        self.__store_number, self.__area_number, self.__dealer_number = self.__activity["crm_dealer_id"].split("_")
        self.__dealer_timezone = self.__activity["dealer_timezone"]

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]

    def remove_credentials(self, payload: str):
        payload = payload.replace(self.__username, "********")
        payload = payload.replace(self.__password, "********")
        return payload

    def retrieve_bad_lead_statuses(self) -> list:
        """Retrieve the bad lead statuses"""
        try:
            s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_REYREY.json"
            s3_object = loads(
                s3_client.get_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )
            return s3_object["bad_lead_status"]
        except Exception as e:
            raise Exception(e)

    def __call_api(self, payload):
        headers = {
            "Content-Type": "application/xml",
        }
        response = requests.post(
            url=self.__url,
            headers=headers,
            data=payload.encode(encoding="UTF-8", errors="ignore"),
        )
        response.raise_for_status()
        logger.info(f"ReyRey response: {response.text}")

        response_dict = xmltodict.parse(response.text)
        response_payload = response_dict["soapenv:Envelope"]["soapenv:Body"]["ProcessMessageResponse"]["payload"]
        trans_status = response_payload["content"]["rey_ImpelCRMUpdateSalesLeadResp"]["TransStatus"]

        status_code = str(trans_status["StatusCode"])
        if status_code != "0":
            logger.error(f"ReyRey responded with an error: {status_code} {trans_status['Status']}")
            raise Exception(f"ReyRey responded with an error: {status_code}")

        crm_activity_id = trans_status["ActivityId"]
        return crm_activity_id

    def convert_utc_to_timezone(self, input_ts: str) -> str:
        """Convert UTC timestamp to dealer's local time."""
        try:
            utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%SZ')
            utc_datetime = pytz.utc.localize(utc_datetime)
        except ValueError:
            utc_datetime = datetime.fromisoformat(input_ts)
            if utc_datetime.tzinfo is None:
                utc_datetime = pytz.utc.localize(utc_datetime)
            else:
                utc_datetime = utc_datetime.astimezone(pytz.utc)

        if not self.__dealer_timezone:
            logger.warning("Dealer timezone not found for crm_dealer_id: {}".format(self.__activity["crm_dealer_id"]))
            return utc_datetime.strftime('%Y-%m-%dT%H:%M:%S')

        # Get the dealer timezone object, convert UTC datetime to dealer timezone
        dealer_tz = pytz.timezone(self.__dealer_timezone)
        dealer_datetime = utc_datetime.astimezone(dealer_tz)

        return dealer_datetime.strftime('%Y-%m-%dT%H:%M:%S')

    def __insert_note(self):
        created_date = self.convert_utc_to_timezone(self.__activity["activity_requested_ts"])
        request_id = str(uuid4())

        payload = REYREY_XML_TEMPLATE.format(
            auth_username=self.__username,
            auth_password=self.__password,
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
        logger.info(f"XML payload to ReyRey: {self.remove_credentials(payload)}")
        return self.__call_api(payload)

    def __create_appointment(self):
        created_date = self.convert_utc_to_timezone(self.__activity["activity_requested_ts"])
        due_date = self.convert_utc_to_timezone(self.__activity["activity_due_ts"])
        request_id = str(uuid4())

        payload = REYREY_XML_TEMPLATE.format(
            auth_username=self.__username,
            auth_password=self.__password,
            bod_id=request_id,
            created_date_time=created_date,
            dealer_number=self.__dealer_number,
            store_number=self.__store_number,
            area_number=self.__area_number,
            prospect_id=self.__activity["crm_lead_id"],
            activity_schema="",
            appointment_schema=APPOINTMENT_SCHEMA.format(
                scheduled_date_time=created_date,
                note=self.__activity["notes"],
                date_time=due_date,
            ),
            note_schema=""
        )
        logger.info(f"XML payload to ReyRey: {self.remove_credentials(payload)}")
        return self.__call_api(payload)

    def __create_activity(self):
        created_date = self.convert_utc_to_timezone(self.__activity["activity_requested_ts"])
        request_id = str(uuid4())

        contact_method = self.__activity["contact_method"].capitalize()
        if contact_method not in ("Phone", "Email", "Text"):
            logger.error(f"ReyRey CRM doesn't support contact method: {contact_method}")
            raise Exception(f"ReyRey CRM doesn't support contact method: {contact_method}")

        payload = REYREY_XML_TEMPLATE.format(
            auth_username=self.__username,
            auth_password=self.__password,
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
        logger.info(f"XML payload to ReyRey: {self.remove_credentials(payload)}")
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
