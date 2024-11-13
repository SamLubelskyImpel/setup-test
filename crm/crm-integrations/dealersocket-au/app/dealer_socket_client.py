import os
import json
import logging
import requests
import hashlib
import hmac
import base64
from os import environ

DEALERSOCKET_ENTITY_URL = os.environ.get("DEALERSOCKET_URL")
DEALERSOCKET_EVENT_URL = os.environ.get("DEALERSOCKET_EVENT_URL")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class DealerSocketClient:
    def __init__(self):
        self.public_key, self.private_key = self.get_secret_keys()

    def get_secret_keys(self):
        """Get secret keys from environment variables"""
        return ["", ""]

    def create_signature(self, body):
        """Create authentication signature based on body"""
        hmac_sha256 = hmac.new(self.private_key.encode(), body.encode(), hashlib.sha256)
        hash_string = base64.b64encode(hmac_sha256.digest()).decode()
        return f"{self.public_key}:{hash_string}"

    def create_xml_body(self, prospect: dict) -> str:
        """Create XML body based on prospect data"""
        xml_body = f"""
        <GetCustomerInformation xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://www.starstandard.org/STAR/5">
            <ApplicationArea>
                <Sender>
                    <CreatorNameCode>VendorName</CreatorNameCode>
                    <SenderNameCode>VendorName</SenderNameCode>
                    <DealerNumberID>938_21</DealerNumberID>
                </Sender>
                <CreationDateTime>2023-10-01T00:00:00</CreationDateTime>
                <Destination>
                    <DestinationNameCode>DS</DestinationNameCode>
                </Destination>
            </ApplicationArea>
            <GetCustomerInformationDataArea>
                <CustomerInformation>
                    <CustomerInformationDetail>
                        <CustomerParty>
                            <SpecifiedPerson>
                                <FamilyName>{prospect['LastName']}</FamilyName>
                                <GivenName>{prospect['FirstName']}</GivenName>
                                <URICommunication>
                                    <URIID>{prospect['Email']}</URIID>
                                    <ChannelCode>Email Address</ChannelCode>
                                </URICommunication>
                                <TelephoneCommunication>
                                    <CompleteNumber>{prospect['HomePhone']}</CompleteNumber>
                                    <UseCode>Home</UseCode>
                                </TelephoneCommunication>
                                <TelephoneCommunication>
                                    <CompleteNumber>{prospect['MobilePhone']}</CompleteNumber>
                                    <UseCode>Mobile</UseCode>
                                </TelephoneCommunication>
                                <TelephoneCommunication>
                                    <CompleteNumber>{prospect['WorkPhone']}</CompleteNumber>
                                    <UseCode>Work</UseCode>
                                </TelephoneCommunication>
                            </SpecifiedPerson>
                        </CustomerParty>
                    </CustomerInformationDetail>
                </CustomerInformation>
            </GetCustomerInformationDataArea>
        </GetCustomerInformation>
        """
        return xml_body

    def query_entity(self, prospect: dict) -> str:
        """Query entity based on prospect data"""
        try:
            xml_body = self.create_xml_body(prospect)
            auth_signature = self.create_signature(xml_body)
            headers = {
                "Content-Type": "application/xml",
                "Authentication": auth_signature
            }
            response = requests.post(
                DEALERSOCKET_ENTITY_URL,
                data=xml_body,
                headers=headers
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error sending post request to {DEALERSOCKET_ENTITY_URL}")
            raise

    def query_event(self, vendor, dealer_id, entity_id) -> dict:
        """Query event based on dealer_id, entity_id, and event"""
        try:
            payload = {
                "vendor": vendor,
                "dealerId": dealer_id,
                "entityId": entity_id
            }
            json_body = json.dumps(payload)
            auth_signature = self.create_signature(json_body)
            headers = {
                "Content-Type": "application/json",
                "Authentication": auth_signature
            }
            response = requests.post(
                DEALERSOCKET_EVENT_URL,
                data=json_body,
                headers=headers
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error sending post request to {DEALERSOCKET_EVENT_URL}")
            raise

# # Load JSON data
# with open('00_123abc.json') as f:
#     data = json.load(f)

# # Extract prospect data
# prospect = data['Prospect']

# # Initialize DealerSocket client
# public_key = "your_public_key"
# private_key = "your_private_key"
# client = DealerSocketClient

# # Query customer
# response = client.query_customer(prospect)
# print(response)
