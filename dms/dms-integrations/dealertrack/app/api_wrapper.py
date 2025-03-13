import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from utils import get_xml_tag
import boto3
import logging
import os
from uuid import uuid4


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

IS_PROD = int(os.environ.get("IS_PROD", 0))
SM = boto3.client('secretsmanager')


class DealerTrackApi:

    def __init__(self, enterprise_code: str, company_number: str):
        self.username, self.password, self.base_url = self.__get_auth()
        self.enterprise_code = enterprise_code
        self.company_number = company_number


    def __get_auth(self):
        # Retrieve secret
        secret_name = "prod/dealertrack" if IS_PROD else "test/dealertrack"
        secret_response = SM.get_secret_value(SecretId=secret_name)
        secret = secret_response['SecretString']
        credentials = eval(secret)  # Assuming the secret is stored as a JSON string
        return credentials['Username'], credentials['Password'], credentials['BaseUrl']


    @property
    def __common_auth_tag(self):
        return f"""
            <Dealer>
                <EnterpriseCode>{self.enterprise_code}</EnterpriseCode>
                <CompanyNumber>{self.company_number}</CompanyNumber>
            </Dealer>"""


    def __call_api(self, api: str, request_body: str, soap_action: str):
        headers = {"SOAPAction": f"opentrack.dealertrack.com/{soap_action}", "accept": "*/*", "Content-Type": "text/xml"}
        xml_body = f'''
            <soap:Envelope
                xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
                <soap:Header>
                    <wsse:Security soap:mustUnderstand="1"
                        xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
                        xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                        <wsse:UsernameToken>
                            <wsse:Username>{self.username}</wsse:Username>
                            <wsse:Password>{self.password}</wsse:Password>
                        </wsse:UsernameToken>
                    </wsse:Security>
                </soap:Header>
                <soap:Body>
                    {request_body}
                </soap:Body>
            </soap:Envelope>'''
        xml_body = ''.join(xml_body.splitlines()).strip()   # Formatting XML for better visualization on Cloudwatch

        response = requests.post(f'{self.base_url}/{api}.asmx', data=xml_body, headers=headers)

        if response.status_code != 200:
            logger.error(f"DMS returned {response.status_code} {response.text} to request {request_body}")

        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the XML response
        root = ET.fromstring(response.content)
        return root


    def vehicle_lookup(self, vin: str):
        xml_body = f'''
            <VehicleLookup
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag}
                <LookupParms>
                    <VIN>{vin}</VIN>
                </LookupParms>
            </VehicleLookup>'''

        xml_root = self.__call_api("vehicleapi", xml_body, "VehicleLookup")
        lookup_result = get_xml_tag(xml_root, 'VehicleLookupResult', first=True)
        return get_xml_tag(lookup_result, 'Result', first=True)


    def customer_lookup(self, customer_number: str):
        xml_body = f'''
            <CustomerLookup
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag}
                <CustomerNumber>{customer_number}</CustomerNumber>
            </CustomerLookup>'''

        xml_root = self.__call_api("customerapi", xml_body, "CustomerLookup")
        lookup_result = get_xml_tag(xml_root, 'CustomerLookupResult', first=True)
        return get_xml_tag(lookup_result, 'Result', first=True)


    def deal_lookup(self, deal_number: str):
        xml_body = f'''
            <DealLookup
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag}
                <LookupParms>
                    <DealNumber>{deal_number}</DealNumber>
                </LookupParms>
            </DealLookup>'''

        xml_root = self.__call_api("dealapi", xml_body, "DealLookup")
        lookup_result = get_xml_tag(xml_root, 'DealLookupResult', first=True)
        return get_xml_tag(lookup_result, 'Result', first=True)


    def ro_details(self, ro_number: str):
        xml_body = f'''
            <GetClosedRepairOrderDetails
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag.replace("Dealer", "dealer")}
                <request>
                    <RepairOrderNumber>{ro_number}</RepairOrderNumber>
                </request>
            </GetClosedRepairOrderDetails>'''

        xml_root = self.__call_api("serviceapi", xml_body, "GetClosedRepairOrderDetails")
        return get_xml_tag(xml_root, 'GetClosedRepairOrderDetailsResponse', first=True, is_ro=True)


    def deal_search(self, date: datetime):
        date_str = date.strftime('%Y-%m-%d')
        xml_body = f'''
            <DealSearch
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag}
                <SearchParms>
                    <OriginationDateStart>{date_str}</OriginationDateStart>
                    <OriginationDateEnd>{date_str}</OriginationDateEnd>
                </SearchParms>
            </DealSearch>'''

        xml_root = self.__call_api("dealapi", xml_body, "DealSearch")
        return get_xml_tag(xml_root, 'DealSearchResult', first=True)


    def ro_lookup(self, date: datetime):
        date_str = date.strftime('%Y-%m-%d')

        xml_body = f'''
            <GetClosedRepairOrders
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag.replace("Dealer", "dealer")}
                <request>
                    <CloseDate>{date_str}</CloseDate>
                </request>
            </GetClosedRepairOrders>'''

        xml_root = self.__call_api("serviceapi", xml_body, "GetClosedRepairOrders")
        result = get_xml_tag(xml_root, 'GetClosedRepairOrdersResponse', first=True, is_ro=True)
        result = get_xml_tag(result, 'GetClosedRepairOrdersResult', first=True, is_ro=True)
        return get_xml_tag(result, 'ClosedRepairOrders', first=True, is_ro=True)


    def appointment_lookup(self, date: datetime):
        date_str = date.strftime('%Y%m%d')

        xml_body = f'''
            <AppointmentLookup
                xmlns="opentrack.dealertrack.com">
                {self.__common_auth_tag}
                <LookupParms>
                    <DateFrom>{date_str}</DateFrom>
                    <DateTo>{date_str}</DateTo>
                </LookupParms>
            </AppointmentLookup>'''

        xml_root = self.__call_api("serviceapi", xml_body, "AppointmentLookup")
        result = get_xml_tag(xml_root, 'AppointmentLookupResult', first=True)
        return get_xml_tag(result, 'Result')
