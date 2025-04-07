import os
import logging
import requests
from os import environ
from json import dumps, loads
import boto3
import xmltodict


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


class PentanaClient:
    """ Client for interacting with the Pentana Lead APIs for DealerSocket integration."""
    def __init__(self):
        secrets = self.get_api_secrets()
        self.base_url = secrets.get("base_url")
        self.userid = secrets.get("userid")
        self.password = secrets.get("password")

    def get_api_secrets(self) -> dict:
        """Retrieve the Pentana Lead API credentials and base URL from Secrets Manager."""
        secret = sm_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret_str = loads(secret["SecretString"])
        pentana_secret = loads(secret_str["PENTANA_DS"])
        return pentana_secret

    def query_event(self, vendor: str, dealer_id: str, entity_id: int) -> dict:
        """Query the Pentana Lead API for lead search."""
        payload = {
            "vendor": vendor,
            "dealerId": dealer_id,
            "entityId": entity_id,
            "eventCategory": "Sales"
        }
        json_body = dumps(payload)
        headers = {
            "Content-Type": "application/json",
            "userid": self.userid,
            "password": self.password,
        }

        url = f"{self.base_url}/EventSearch/PostLead"
        try:
            response = requests.post(url, data=json_body, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error sending post request to Pentana EventSearch endpoint: {e}")
            raise

    def query_entity(self, vendor, dealer_id, prospect: dict) -> list:
        """
        Query customer based on prospect data using Pentana's Lead Search endpoint.
        This function will try each PII field (email, then mobile phone, then last name)
        by making separate API calls. It aggregates all customer records (i.e. those
        responses whose XML contains "CustomerInformation") into one list, deduplicates
        them by PartyID, and returns the final list.

        Raises:
        Exception if no valid customer data is returned for any of the provided PII fields.
        """

        potential_fields = []
        if prospect.get("Email"):
            potential_fields.append(prospect["Email"])
        if prospect.get("MobilePhone"):
            potential_fields.append(prospect["MobilePhone"])
        if prospect.get("HomePhone"):
            potential_fields.append(prospect["HomePhone"])
        if prospect.get("LastName"):
            potential_fields.append(prospect["LastName"])

        if not potential_fields:
            raise ValueError("No valid PII field found in prospect for search")

        headers = {
            "Content-Type": "application/json",
            "userid": self.userid,
            "password": self.password,
            "creatorNameCode": "Pentana",
            "dealerNumberId": dealer_id
        }
        url = f"{self.base_url}/Customer/SearchCustomer"
        aggregated_records = []

        for field in potential_fields:
            params = {
                "department": "a",
                "searchstring": field
            }

            response = requests.post(url, params=params, headers=headers)
            response.raise_for_status()
            entity_xml = response.text

            entity_dict = xmltodict.parse(entity_xml)
            data_area = entity_dict.get("ShowCustomerInformation", {}) \
                .get("ShowCustomerInformationDataArea", {})

            if "CustomerInformation" in data_area:
                customer_info = data_area.get("CustomerInformation")
                if not isinstance(customer_info, list):
                    customer_info = [customer_info]
                logger.info(f"Found customer data using searchstring: {field} ({len(customer_info)} records)")
                aggregated_records.extend(customer_info)
            else:
                logger.info(f"No customer data returned for searchstring: {field}")

        if not aggregated_records:
            raise Exception("Empty customer data returned from API for all provided PII fields")

        logger.info(f"Aggregated customer records: {aggregated_records}")
        
        # Deduplicate records based on PartyID
        seen = set()
        deduped_records = []
        for record in aggregated_records:
            party_id = record.get("CustomerInformationDetail", {}) \
                            .get("CustomerParty", {}) \
                            .get("PartyID")
            if party_id and party_id not in seen:
                seen.add(party_id)
                deduped_records.append(record)

        logger.info(f"Aggregated records count: {len(aggregated_records)}, deduplicated to {len(deduped_records)} records.")
        return deduped_records
