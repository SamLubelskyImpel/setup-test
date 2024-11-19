"""
These classes are designed to manage calls to the Dealersocket AU/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import pytz
import logging
import requests
from os import environ
from hmac import new
from json import loads
from boto3 import client
from hashlib import sha256
from base64 import b64encode
from datetime import datetime
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
OUTBOUND_CALL_DEFAULT_MESSAGE = "Sales AI email/text sent. Clock stopped."

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")


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

    def __run_get(self, endpoint: str):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/{endpoint}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            },
        )
        response.raise_for_status()
        return response.json()

    def get_salesperson(self, lead_id: int):
        salespersons = self.__run_get(f"leads/{lead_id}/salespersons")
        if not salespersons:
            return None

        return salespersons[0]

    def update_activity(self, activity_id, crm_activity_id):
        try:
            response = requests.put(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                json={"crm_activity_id": crm_activity_id},
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            response.raise_for_status()
            logger.info(f"CRM API PUT Activities responded with: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")

    def get_appointments(self, lead_id):
        response_json = self.__run_get(f"leads/{lead_id}/activities")
        return [activity for activity in response_json if activity["activity_type"] == "appointment"]

class DealersocketAUApiWrapper:
    """Improved Dealersocket AU API Wrapper with Appointment Update Handling."""

    def __init__(self, activity: dict, salesperson: dict):
        self.activity = activity
        self.salesperson = salesperson
        self.dealer_timezone = activity.get("dealer_timezone")
        self.private_key, self.public_key, self.api_url = self._load_secrets()

    def _load_secrets(self):
        """Load API credentials from the secret manager."""
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret_data = loads(loads(secret["SecretString"])[SECRET_KEY])
        return (
            secret_data["API_PRIVATE_KEY"],
            secret_data["API_PUBLIC_KEY"],
            secret_data["API_URL"],
        )

    def _create_signature(self, payload: str) -> str:
        """Generate HMAC signature for API requests."""
        hmac_sha256 = new(self.private_key.encode(), payload.encode(), sha256)
        hash_string = b64encode(hmac_sha256.digest()).decode("utf-8")
        return f"{self.public_key}:{hash_string}"

    def _send_request(self, endpoint: str, payload: str, method: str = "POST"):
        """Send API request to Dealersocket."""
        headers = {
            "Content-Type": "application/xml",
            "Authentication": self._create_signature(payload),
        }
        url = f"{self.api_url}/{endpoint}"
        logger.info(f"Sending {method} request to {url}")
        response = requests.request(method=method, url=url, data=payload, headers=headers)
        response.raise_for_status()
        return response.text

    def _build_xml(self, root_tag: str, elements: dict) -> str:
        """Build XML payload dynamically."""
        root = Element(root_tag, {
            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance"
        })
        for tag, value in elements.items():
            if value is not None:  # Skip empty elements
                sub_element = SubElement(root, tag)
                sub_element.text = value
        return tostring(root, encoding="unicode")

    def _parse_response(self, response_text: str) -> dict:
        """Parse XML response into a dictionary."""
        root = fromstring(response_text)
        return {
            self._strip_namespace(tag.tag): tag.text
            for tag in root
        }

    @staticmethod
    def _strip_namespace(tag: str) -> str:
        """Remove XML namespace from tag."""
        return tag.split("}")[-1]

    def _convert_to_dealer_timezone(self, utc_ts: str) -> str:
        """Convert UTC timestamp to dealer's local timezone."""
        utc_datetime = pytz.utc.localize(datetime.strptime(utc_ts, '%Y-%m-%dT%H:%M:%SZ'))
        if not self.dealer_timezone:
            logger.warning(f"Dealer timezone not found for crm_dealer_id: {self.activity['crm_dealer_id']}")
            return utc_datetime.isoformat()
        dealer_tz = pytz.timezone(self.dealer_timezone)
        return utc_datetime.astimezone(dealer_tz).isoformat()

    def _get_common_elements(self) -> dict:
        """Return common elements used in multiple payloads."""
        return {
            "Vendor": "Impel",
            "DealerId": self.activity["crm_dealer_id"],
            "EntityId": self.activity["crm_consumer_id"],
            "EventId": self.activity["crm_lead_id"],
            "Note": f"<![CDATA[{self.activity['notes']}]]>",
        }

    def _create_activity_payload(self, activity_type: str, activity_id: str = "") -> str:
        """Generate payload for a specific activity type."""
        common_elements = self._get_common_elements()

        if activity_type == "note":
            common_elements.update({
                "BatchId": "0",
            })
            return self._build_xml("WorkNoteInsert", common_elements)

        elif activity_type == "appointment":
            common_elements.update({
                "ActivityType": "Appointment",
                "Status": "Open",
                "DueDateTime": self._convert_to_dealer_timezone(self.activity["activity_due_ts"]),
                "AssignedToUser": self.salesperson["crm_salesperson_id"],
            })
            if activity_id:
                common_elements["ActivityId"] = activity_id
            return self._build_xml("ActivityInsert", common_elements)

        elif activity_type == "outbound_call":
            common_elements.update({
                "ActivityType": "Outbound_Call",
                "Status": "Completed",
                "DueDateTime": self._convert_to_dealer_timezone(datetime.now().isoformat()),
                "AssignedToUser": self.salesperson["crm_salesperson_id"],
            })
            return self._build_xml("ActivityInsert", common_elements)

        else:
            raise ValueError(f"Unsupported activity type: {activity_type}")

    def _handle_activity_exists(self, error_message: str) -> str:
        """Extract ActivityId and retry with updated payload."""
        activity_id = error_message.split("ActivityId:")[-1].strip()
        logger.info(f"Existing activity detected, retrying with ActivityId: {activity_id}")
        updated_payload = self._create_activity_payload("appointment", activity_id=activity_id)
        return self._send_request("Activity", updated_payload, method="PUT")

    def create_activity(self):
        """Create an activity on CRM."""
        try:
            activity_type = self.activity["activity_type"]
            payload = self._create_activity_payload(activity_type)
            endpoint = "WorkNote" if activity_type == "note" else "Activity"
            response_text = self._send_request(endpoint, payload)
            response_data = self._parse_response(response_text)

            # Handle ACTIVITY_EXISTS for appointments
            if activity_type == "appointment" and response_data.get("ErrorCode") == "ACTIVITY_EXISTS":
                return self._parse_response(
                    self._handle_activity_exists(response_data["ErrorMessage"])
                )

            return response_data
        except Exception as e:
            logger.error(f"Failed to create activity: {e}")
            return None


# class DealersocketAUApiWrapper:
#     """Dealersocket AU API Wrapper."""

#     def __init__(self, **kwargs):
#         self.__private_key, self.__public_key, self.__api_url  = self.get_secrets()

#         self.__activity = kwargs.get("activity")
#         self.__salesperson = kwargs.get("salesperson")
#         self.__dealer_timezone = self.__activity.get("dealer_timezone")

#     def get_secrets(self):
#         secret = secret_client.get_secret_value(
#             SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
#         )
#         secret = loads(secret["SecretString"])[str(SECRET_KEY)]
#         secret_data = loads(secret)

#         return (
#             secret_data["API_PRIVATE_KEY"],
#             secret_data["API_PUBLIC_KEY"],
#             secret_data["API_URL"]
#         )

#     def __create_signature(self, body: str) -> str:
#         hmac_sha256 = new(self.__private_key.encode('utf-8'), body.encode('utf-8'), sha256)
#         hash_bytes = hmac_sha256.digest()
#         hash_string = b64encode(hash_bytes).decode('utf-8')
        
#         return f"{self.__public_key}:{hash_string}"

#     def __call_api(self, url, payload, method="POST"):
#         headers = {
#             'Content-Type': 'application/xml',
#             'Authentication': self.__create_signature(payload)
#         }
#         response = requests.request(
#             method=method,
#             url=url,
#             data=payload.encode(encoding="UTF-8"),
#             headers=headers,
#         )
#         logger.info(f"Response from CRM: {response.status_code}")
#         return response

#     def __remove_xml_namespace_from_tag(self, tag: str) -> str:
#         try:
#             tag = tag[tag.index("}") + 1 :]
#             return tag
#         except ValueError as exc:
#             return tag
        
#     def __parse_dealersocket_xml_response(self, xml_str: str) -> dict:
#         return_dict = {}
#         root = xml.etree.ElementTree.fromstring(xml_str)
#         for iterator in root:
#             get_normalized_tag = self.__remove_xml_namespace_from_tag(iterator.tag)
#             return_dict[get_normalized_tag] = iterator.text
#         return return_dict

#     def convert_utc_to_timezone(self, input_ts: str) -> Tuple[str, str]:
#         """Convert UTC timestamp to dealer's local time."""
#         utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%SZ')
#         utc_datetime = pytz.utc.localize(utc_datetime)

#         if not self.__dealer_timezone:
#             logger.warning("Dealer timezone not found for crm_dealer_id: {}".format(self.__activity["crm_dealer_id"]))
#             new_ts = utc_datetime.isoformat()
#         else:
#             # Get the dealer timezone object, convert UTC datetime to dealer timezone
#             dealer_tz = pytz.timezone(self.__dealer_timezone)
#             dealer_datetime = utc_datetime.astimezone(dealer_tz)
#             new_ts = dealer_datetime.isoformat()

#         return new_ts

#     def __create_appointment(self):
#         """Create appointment on CRM."""
#         url = "{}/Activity".format(self.__api_url)
#         appt_date_time = self.convert_utc_to_timezone(self.__activity["activity_due_ts"])

#         appt_template = """
#             <ActivityInsert xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
#                 <Vendor>Impel</Vendor>
#                 <DealerId>{dealer_id}</DealerId>
#                 <ActivityType>Appointment</ActivityType>
#                 <EntityId>{customer_id}</EntityId>
#                 <EventId>{lead_id}</EventId>
#                 <Status>Open</Status>
#                 <DueDateTime>{appt_date_time}</DueDateTime>
#                 <AssignedToUser>{salesperson}</AssignedToUser>
#                 <Note>
#                     <![CDATA[{note}]]>
#                 </Note>
#                 {activity_id}
#             </ActivityInsert>
#         """

#         payload = appt_template.format(
#             dealer_id=self.__activity['crm_dealer_id'],
#             customer_id=self.__activity['crm_consumer_id'],
#             lead_id=self.__activity['crm_lead_id'],
#             appt_date_time=appt_date_time,
#             salesperson=self.__salesperson["crm_salesperson_id"],
#             note=self.__activity['notes'],
#             activity_id=""
#         )

#         logger.info(f"Payload to CRM: {payload}")
#         response = self.__call_api(url, payload)
#         logger.info("[dealersocket_appointment] response text", response.text)
#         dealersocket_response_dict = self.__parse_dealersocket_xml_response(response.text)
#         error_code = dealersocket_response_dict.get("ErrorCode", "")
#         # check if activity already exists
#         if error_code == "ACTIVITY_EXISTS":
#             print("INFO [dealersocket_insert_appointment] update appointment")
#             activity_id = dealersocket_response_dict.get("ErrorMessage").split(
#                 "ActivityId:"
#             )[-1]
#             payload = appt_template.format(
#                 dealer_id=self.__activity['crm_dealer_id'],
#                 customer_id=self.__activity['crm_consumer_id'],
#                 lead_id=self.__activity['crm_lead_id'],
#                 appt_date_time=appt_date_time,
#                 salesperson=self.__salesperson["crm_salesperson_id"],
#                 note=self.__activity['notes'],
#                 activity_id=activity_id
#             )
            
#             logger.info(f"Payload to CRM: {payload}")
#             response = self.__call_api(url, payload, "PUT")
            
#             logger.info("[dealersocket_appointment] response text", response.text)
#             dealersocket_response_dict = self.__parse_dealersocket_xml_response(response.text)
#             logger.info(
#                 "INFO [dealersocket_insert_appointment] update appointment response text",
#                 response.text,
#             )
#         response.raise_for_status()

#         return dealersocket_response_dict

#     def __insert_note(self):
#         """Insert note on CRM."""

#         url = "{}/WorkNote".format(self.__api_url)

#         payload = """
#             <WorkNoteInsert xmlns:xsd="http://www.w3.org/2001/XMLSchema"
#                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
#                 <Vendor>Impel</Vendor>
#                 <DealerId>{dealer_id}</DealerId>
#                 <EntityId>{customer_id}</EntityId>
#                 <EventId>{lead_id}</EventId>
#                 <BatchId>0</BatchId>
#                 <Note>
#                     <![CDATA[{note}]]>
#                 </Note>
#             </WorkNoteInsert>
#         """.format(
#             dealer_id=self.__activity['crm_dealer_id'],
#             customer_id=self.__activity['crm_consumer_id'],
#             lead_id=self.__activity['crm_lead_id'],
#             note=self.__activity['notes']
#         )
#         logger.info(f"Payload to CRM: {payload}")

#         response = self.__call_api(url, payload)
#         response.raise_for_status()
#         logger.info("INFO [dealersocket_worknote] response text", response.text)
#         return self.__parse_dealersocket_xml_response(response.text)

#     def __create_outbound_call(self):
#         """Create outbound call on CRM."""
#         url = "{}/Activity".format(self.__api_url)

#         outbound_call_time = self.convert_utc_to_timezone(datetime.now().isoformat())

#         payload = """
#             <ActivityInsert xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
#                 <Vendor>Impel</Vendor>
#                 <DealerId>{dealer_id}</DealerId>
#                 <ActivityType>Outbound_Call</ActivityType>
#                 <EntityId>{customer_id}</EntityId>
#                 <EventId>{lead_id}</EventId>
#                 <Status>Completed</Status>
#                 <DueDateTime>{due_datetime}</DueDateTime>
#                 <AssignedToUser>{salesperson}</AssignedToUser>
#                 <Note>
#                     <![CDATA[{note}]]>
#                 </Note>
#             </ActivityInsert>
#         """.format(
#             dealer_id=self.__activity['crm_dealer_id'],
#             customer_id=self.__activity['crm_consumer_id'],
#             lead_id=self.__activity['crm_lead_id'],
#             due_datetime=outbound_call_time,
#             salesperson=self.__salesperson["crm_salesperson_id"],
#             note=self.__activity['notes']
#         )
#         logger.info(f"Payload to CRM: {payload}")

#         response = self.__call_api(url, payload)
#         response.raise_for_status()

#         logger.info("INFO [dealersocket_outbound_call] response text", response.text)
#         return self.__parse_dealersocket_xml_response(response.text)

#     def create_activity(self):
#         """Create activity on CRM."""
#         if self.__activity["activity_type"] == "note":
#             return self.__insert_note()
#         elif self.__activity["activity_type"] == "appointment":
#             return self.__create_appointment()
#         elif self.__activity["activity_type"] == "outbound_call":
#             return self.__create_outbound_call()
#         else:
#             logger.error(
#                 f"Dealersocket AU CRM doesn't support activity type: {self.__activity['activity_type']}"
#             )
#             return None
