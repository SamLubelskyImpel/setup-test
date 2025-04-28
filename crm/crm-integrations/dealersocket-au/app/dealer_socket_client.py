import os
import logging
import requests
import hashlib
import hmac
import base64
from os import environ
from json import dumps, loads
from datetime import datetime

import boto3

DEALERSOCKET_ENTITY_URL = os.environ.get("DEALERSOCKET_URL")
DEALERSOCKET_EVENT_URL = os.environ.get("DEALERSOCKET_EVENT_URL")

ENVIRONMENT = os.environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

sm_client = boto3.client("secretsmanager")

DS_LEAD_STATUS_MAPPINGS = {
    220: "Unqualified",
    221: "Up/Contacted",
    227: "Store Visit",
    222: "Demo Vehicle",
    223: "Write Up",
    224: "Pending F&I",
    225: "Sold",
    226: "Lost"
}


class DealerSocketClient:
    """The DealerSocket client class has functions to query the dealersocket APIs."""
    def __init__(self):
        self.public_key, self.private_key = self.get_secret_keys()

    def get_secret(self, secret_name, secret_key):
        """Get secret from Secrets Manager."""
        secret = sm_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
        )
        secret = loads(secret["SecretString"])[str(secret_key)]
        secret_data = loads(secret)
        return secret_data

    def get_secret_keys(self):
        """Get secret keys"""
        dealersocket_secrets = self.get_secret("crm-integrations-partner", "DEALERSOCKET_AU")
        return dealersocket_secrets.get("public_key"), dealersocket_secrets.get("private_key")

    def create_signature(self, body):
        """Create authentication signature based on body"""
        hmac_sha256 = hmac.new(self.private_key.encode(), body.encode(), hashlib.sha256)
        hash_string = base64.b64encode(hmac_sha256.digest()).decode()
        return f"{self.public_key}:{hash_string}"

    def create_xml_body(self, vendor, dealer_id, prospect: dict) -> str:
        """Create XML body based on prospect data"""
        email_section = ""
        if prospect.get('Email'):
            email_section = f"""
                <URICommunication>
                    <URIID>{prospect['Email']}</URIID>
                    <ChannelCode>Email Address</ChannelCode>
                </URICommunication>"""

        phone_sections = ""
        phone_keys = ['MobilePhone', 'HomePhone', 'WorkPhone']

        for phone_key in phone_keys:
            if prospect.get(phone_key):
                phone_sections += f"""
                    <TelephoneCommunication>
                        <CompleteNumber>{prospect[phone_key]}</CompleteNumber>
                    </TelephoneCommunication>"""
                break

        family_name_section = ""
        if prospect.get('LastName'):
            family_name_section = f"""
                <FamilyName>{prospect['LastName']}</FamilyName>"""

        given_name_section = ""
        if prospect.get('FirstName'):
            given_name_section = f"""
                <GivenName>{prospect['FirstName']}</GivenName>"""

        xml_body = f"""
        <GetCustomerInformation xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://www.starstandard.org/STAR/5">
            <ApplicationArea>
                <Sender>
                    <CreatorNameCode>{vendor}</CreatorNameCode>
                    <SenderNameCode>{vendor}</SenderNameCode>
                    <DealerNumberID>{dealer_id}</DealerNumberID>
                </Sender>
                <CreationDateTime>{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}</CreationDateTime>
                <Destination>
                    <DestinationNameCode>DS</DestinationNameCode>
                </Destination>
            </ApplicationArea>
            <GetCustomerInformationDataArea>
                <CustomerInformation>
                    <CustomerInformationDetail>
                        <CustomerParty>
                            <SpecifiedPerson>
                                {family_name_section}
                                {given_name_section}
                                {email_section}
                                {phone_sections}
                            </SpecifiedPerson>
                        </CustomerParty>
                    </CustomerInformationDetail>
                </CustomerInformation>
            </GetCustomerInformationDataArea>
        </GetCustomerInformation>
        """
        return xml_body

    def query_entity(self, vendor, dealer_id, prospect: dict) -> str:
        """Query entity based on prospect data"""
        try:
            xml_body = self.create_xml_body(vendor, dealer_id, prospect)
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
        except Exception:
            logger.error(f"Error sending post request to {DEALERSOCKET_ENTITY_URL}")
            raise

    def query_event(self, vendor, dealer_id, entity_id) -> dict:
        """Query event based on dealer_id, entity_id, and event"""
        try:
            payload = {
                "vendor": vendor,
                "dealerId": dealer_id,
                "entityId": entity_id,
                "eventCategory": "Sales"
            }
            json_body = dumps(payload)
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
        except Exception:
            logger.error(f"Error sending post request to {DEALERSOCKET_EVENT_URL}")
            raise
