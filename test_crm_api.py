import requests
import logging
from requests.exceptions import HTTPError

import os
# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
API_KEY=os.environ.get('API_KEY', 'test_6d6407b333e04fbaa42e7ca6549ff0dd')
print(API_KEY)


class CRMAPIWrapper:
    def __init__(self, base_url, partner_id, api_key):
        self.base_url = base_url
        self.headers = {
            'partner_id': partner_id,
            'x_api_key': api_key,
            'Content-Type': 'application/json'
        }

    def get_salespersons(self, dealer_id):
        """
        Fetches the list of salespersons for a given dealer.
        
        :param dealer_id: The ID of the dealer (e.g., for PBS)
        :return: JSON response with salesperson details or error information.
        """
        url = f"{self.base_url}/dealers/{dealer_id}/salespersons"
        try:
            logger.info(f"Fetching salespersons for dealer_id: {dealer_id}")
            response = requests.get(url, headers=self.headers)

            # Raise an HTTPError if the status is 4xx or 5xx
            response.raise_for_status()

            logger.info("Successfully fetched salespersons data.")
            return response.json()

        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            return {"error": str(http_err), "status_code": response.status_code}
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            return {"error": str(err)}

# Example usage of the wrapper
if __name__ == "__main__":
    # Replace these with actual values

    BASE_URL = "https://crm-api-test.testenv.impel.io"  # Test environment URL
    PARTNER_ID = "test"  # Replace with actual partner_id for PBS
    DEALER_ID = "pbs-test-dealer"  # Replace with PBS's actual dealer ID

    crm_api = CRMAPIWrapper(BASE_URL, PARTNER_ID, API_KEY)

    # Fetch salespersons for PBS dealer
    salespersons_data = crm_api.get_salespersons(DEALER_ID)
    print(salespersons_data)
    # Print the response (for debugging or logging)
    logger.info(f"Salespersons data: {salespersons_data}")
