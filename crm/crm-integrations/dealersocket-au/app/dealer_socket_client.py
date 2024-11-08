import json
import requests
import hashlib
import hmac
import base64
import os

DEALERSOCKET_URL = os.environ.get("DEALERSOCKET_URL")


class DealerSocketClient:
    def __init__(self, public_key, private_key):
        self.public_key = public_key
        self.private_key = private_key
        self.url = DEALERSOCKET_URL

    def create_signature(self, body):
        hmac_sha256 = hmac.new(self.private_key.encode(), body.encode(), hashlib.sha256)
        hash_string = base64.b64encode(hmac_sha256.digest()).decode()
        return f"{self.public_key}:{hash_string}"

    def create_xml_body(self, prospect):
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
                            </SpecifiedPerson>
                        </CustomerParty>
                    </CustomerInformationDetail>
                </CustomerInformation>
            </GetCustomerInformationDataArea>
        </GetCustomerInformation>
        """
        return xml_body

    def query_customer(self, prospect):
        xml_body = self.create_xml_body(prospect)
        auth_signature = self.create_signature(xml_body)
        headers = {
            "Content-Type": "application/xml",
            "Authentication": auth_signature
        }
        response = requests.post(self.url, data=xml_body, headers=headers)
        if response.status_code == 200:
            return response.text
        else:
            return f"Error: {response.status_code} {response.text}"

# Load JSON data
with open('00_123abc.json') as f:
    data = json.load(f)

# Extract prospect data
prospect = data['Prospect']

# Initialize DealerSocket client
public_key = "your_public_key"
private_key = "your_private_key"
client = DealerSocketClient(public_key, private_key)

# Query customer
response = client.query_customer(prospect)
print(response)