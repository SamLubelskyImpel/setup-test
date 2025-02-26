from typing import Any, Dict
import logging
from os import environ
from json import dumps, loads
import boto3
from requests import get


ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


def get_tekion_secrets():
    """Get Tekion API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    token = loads(s3_client.get_object(Bucket=BUCKET, Key="tekion_crm/token.json")["Body"].read().decode('utf-8'))

    return secret_data["url"], secret_data["app_id"], token["token"]


def hit_tekion_api(endpoint, params, dealer_id):
    """Hit Tekion API."""
    api_url, client_id, access_key = get_tekion_secrets()
    headers = {
        "app_id": client_id,
        "Authorization": f"Bearer {access_key}",
        "dealer_id": dealer_id
    }
    response = get(
        url=f"{api_url}/{endpoint}",
        headers=headers,
        params=params
    )
    logger.info(f"Tekion responded with: {response.status_code}, {response.text}")
    response.raise_for_status()
    return response.json()


def get_dealer_salespersons(crm_dealer_id):
    """Fetch and return the dealer's salespersons list from Tekion."""
    
    salespersons = []
    roles = ["SALES_PERSON", "SALES_MANAGER", "BDC_MANAGER"]

    for role in roles:
        params = {"persona": role}

        while True:
            response = hit_tekion_api("openapi/v4.0.0/users", params, crm_dealer_id)

            if response.get("meta", {}).get("status", "").lower() != "success":
                logger.error(f"Error fetching salespersons for {role}: {response}")
                break  # Stop fetching for this role

            for user in response.get("data", []):
                if user.get("active"):
                    salespersons.append({
                        "crm_salesperson_id": user["id"],
                        "first_name": user.get("userNameDetails", {}).get("firstName"),
                        "last_name": user.get("userNameDetails", {}).get("lastName"),
                        "email": user.get("email"),
                        "phone": None,  # Phone number is not provided in the response
                        "position_name": user.get("userRoleDetails", {})
                                         .get("primaryRoleDetails", {})
                                         .get("roleName", role),
                    })

            next_fetch_key = response.get("meta", {}).get("nextFetchKey")
            if not next_fetch_key:
                break  # No more pages to fetch

            params["nextFetchKey"] = next_fetch_key  # Update params for next API call

    logger.info(f"Found {len(salespersons)} salespersons.")
    return salespersons


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Get dealer's salespersons."""
    logger.info(f"Event: {event}")

    crm_dealer_id = event["crm_dealer_id"]

    try:
        salespersons = get_dealer_salespersons(crm_dealer_id)
        return {
            "statusCode": 200,
            "body": dumps(salespersons)
        }
    except Exception as e:
        logger.exception(f"Error retrieving dealer's salespersons: {str(e)}")
        raise
