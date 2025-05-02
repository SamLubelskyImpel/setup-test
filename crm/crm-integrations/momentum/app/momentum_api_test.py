"""
dev use only. should be deleted before merge.
"""

import requests
import logging
import argparse
import json
from os import environ

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Hardcoded values - replace these with your actual values
MOMENTUM_LOGIN = "https://momapi.udccrm.com:8443"
MOMENTUM_MASTER_KEY_PROD = environ.get("MOMENTUM_MASTER_KEY_PROD")
MOMENTUM_MASTER_KEY_TEST = environ.get("MOMENTUM_MASTER_KEY_TEST")

# Test environment IDs
TEST_DEALER_ID = "37f2cf95-fcf3-45c0-af54-57c1e7bef8c5"
TEST_PERSON_ID = "361d6d64-6c3a-4724-9024-cb942f0e66cd"

# Production environment IDs
PROD_DEALER_ID = "d69d0cdc-2440-4903-9e58-6f505afa175a"  # Replace with actual production dealer ID
PROD_PERSON_ID = "8d8c8661-7746-4523-a12f-855bd4b94a8d"  # Replace with actual production person ID

# API versions
API_VERSION_TEST = "v2.1"
API_VERSION_PROD = "v2.1"

class MomentumApiWrapper:
    """Simplified Momentum API Wrapper for testing."""

    def __init__(self, **kwargs):
        self.__login_url = MOMENTUM_LOGIN
        self.__master_key = kwargs.get("master_key", MOMENTUM_MASTER_KEY_TEST)
        self.__dealer_id = kwargs.get("dealer_id")
        self.__api_version = kwargs.get("api_version", API_VERSION_TEST)
        self.__api_token, self.__api_url = self.get_token()

    def get_token(self):
        url = f"{self.__login_url}/{self.__dealer_id}"
        logger.info(f"Making token request to URL: {url}")
        logger.info(f"Using master key: {self.__master_key}")
        
        headers = {
            "Content-Type": "application/json",
            "MOM-ApplicationType": "V",
            "MOM-Api-Key": self.__master_key,
        }
        logger.info(f"Request headers: {json.dumps(headers, indent=2)}")
        
        response = requests.get(url=url, headers=headers)
        logger.info(f"Token request response status: {response.status_code}")
        
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Token response: {json.dumps(response_json, indent=2)}")

        api_token = response_json["apiToken"]
        api_url = response_json["endPoint"]
        logger.info(f"Retrieved API token: {api_token}")
        logger.info(f"Retrieved API URL: {api_url}")
        
        return api_token, api_url

    def __call_api(self, url, payload=None, method="POST"):
        logger.info(f"\n{'='*50}")
        logger.info(f"Making {method} request to URL: {url}")
        
        headers = {
            "Content-Type": "application/json",
            "MOM-ApplicationType": "V",
            "MOM-Api-Token": self.__api_token,
        }
        logger.info(f"Request headers: {json.dumps(headers, indent=2)}")
        
        if payload:
            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")
        
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
        )
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {json.dumps(dict(response.headers), indent=2)}")
        
        try:
            response_json = response.json()
            logger.info(f"Response body: {json.dumps(response_json, indent=2)}")
        except:
            logger.info(f"Response body: {response.text}")
        
        logger.info(f"{'='*50}\n")
        return response

    def get_alternate_person_ids(self, person_api_id: str):
        """Retrieve all associated personApiIDs for a given consumer."""
        url = f"{self.__api_url}/{self.__api_version}/customer/personApiID/{person_api_id}"
        logger.info(f"Looking up alternate person IDs for person: {person_api_id}")
        
        response = self.__call_api(url=url, method="GET")
        response.raise_for_status()
        response_json = response.json()
        return response_json

# Example usage:
if __name__ == "__main__":
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Test Momentum API wrapper')
    parser.add_argument('--prod', action='store_true', help='Use production environment')
    parser.add_argument('--dealer-id', help='Override dealer ID')
    parser.add_argument('--person-id', help='Override person ID')
    parser.add_argument('--api-version', help='Override API version')
    args = parser.parse_args()

    # Select environment-specific values
    is_prod = args.prod
    master_key = MOMENTUM_MASTER_KEY_PROD if is_prod else MOMENTUM_MASTER_KEY_TEST
    dealer_id = args.dealer_id or (PROD_DEALER_ID if is_prod else TEST_DEALER_ID)
    person_id = args.person_id or (PROD_PERSON_ID if is_prod else TEST_PERSON_ID)
    api_version = args.api_version or (API_VERSION_PROD if is_prod else API_VERSION_TEST)

    logger.info(f"Using {'production' if is_prod else 'test'} environment")
    logger.info(f"Using master key: {master_key}")
    logger.info(f"Using dealer ID: {dealer_id}")
    logger.info(f"Looking up person ID: {person_id}")
    logger.info(f"Using API version: {api_version}")

    # Initialize the wrapper with the selected values
    momentum_api = MomentumApiWrapper(
        dealer_id=dealer_id,
        master_key=master_key,
        api_version=api_version
    )
    
    # Test getting alternate person IDs
    try:
        result = momentum_api.get_alternate_person_ids(person_id)
        logger.info(f"Alternate person IDs retrieved successfully: {json.dumps(result, indent=2)}")
    except Exception as e:
        logger.error(f"Error retrieving alternate person IDs: {e}") 