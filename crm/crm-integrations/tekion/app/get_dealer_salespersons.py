from typing import Any, Dict, Optional
import logging
from os import environ
from json import dumps, loads, JSONDecodeError
import boto3
from requests import get, RequestException


ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CONFIG_FILE_KEY = "configurations/tekion_api_version_config.json"
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN", "crm-api-test.testenv.impel.io")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY", "impel")

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

def fetch_s3_file(bucket: str, key: str) -> Optional[Dict]:
    """Retrieve and parse JSON file from S3."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        logger.error(f"S3 file not found: {key}")
    except Exception as e:
        logger.exception(f"Failed to fetch S3 file {key}: {e}")
    return None

def get_secret(secret_id: str) -> Optional[Dict]:
    """Fetch and parse secret from AWS Secrets Manager."""
    try:
        secret = secret_client.get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except secret_client.exceptions.ResourceNotFoundException:
        logger.error(f"Secret not found: {secret_id}")
    except Exception as e:
        logger.exception(f"Failed to retrieve secret {secret_id}: {e}")
    return None

def get_api_version_config():
    """Retrieve API version configuration from S3."""
    try:
        response = s3_client.get_object(Bucket=BUCKET, Key=CONFIG_FILE_KEY)
        content = response['Body'].read().decode('utf-8')
        return loads(content)
    except Exception as e:
        logger.error(f"Failed to fetch API version config: {e}")
        return {}
    
def get_product_dealer_id(crm_dealer_id: str) -> Optional[str]:
    """Fetch product dealer ID from CRM API."""
    secret_data = get_secret(f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api")
    if not secret_data:
        return None

    try:
        api_key = loads(secret_data[CRM_API_SECRET_KEY])["api_key"]
        response = get(
            url=f"https://{CRM_API_DOMAIN}/dealers/config",
            params={"crm_dealer_id": crm_dealer_id},
            headers={"x_api_key": api_key, "partner_id": CRM_API_SECRET_KEY},
        )
        response.raise_for_status()
        return response.json()[0].get("product_dealer_id", "")
    except RequestException as e:
        logger.exception(f"Failed to get product dealer ID: {e}")
    return None

def get_tekion_secrets() -> Optional[tuple]:
    """Retrieve Tekion API credentials from Secrets Manager and S3."""
    secret_data = get_secret(f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner")
    token_data = fetch_s3_file(BUCKET, "tekion_crm/token.json")

    if not secret_data or not token_data:
        return None

    try:
        tekion_secret = loads(secret_data[str(SECRET_KEY)])
        return tekion_secret["url"], tekion_secret["app_id"], token_data["token"]
    except (KeyError, JSONDecodeError) as e:
        logger.exception(f"Error parsing Tekion secrets: {e}")
    return None

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
    version_config = get_api_version_config()
    product_dealer_id = get_product_dealer_id(crm_dealer_id)
    api_version = version_config.get(product_dealer_id, "v3")
    
    salespersons = []
    roles = ["SALES_PERSON", "SALES_MANAGER"]

    roles.append("BDC_MANAGER" if api_version == "v4" else "BDCManager")

    for role in roles:
        params = {"persona": role} if api_version == "v4" else {"isActive": "true", "role": role}

        while True:
            response = hit_tekion_api(
                "openapi/v4.0.0/users" if api_version == "v4" else "openapi/v3.1.0/employees", 
                params, 
                crm_dealer_id
            )


            
            if api_version == "v4":
                if response.get("meta", {}).get("status", "").lower() != "success":
                    logger.error(f"Error fetching salespersons for role: {role}.")
                    break  # Stop fetching for this role
                for user in response.get("data", []):
                    if user.get("active"):
                        salespersons.append({
                            "crm_salesperson_id": user["id"],
                            "first_name": user.get("userNameDetails", {}).get("firstName", ""),
                            "last_name": user.get("userNameDetails", {}).get("lastName", ""),
                            "email": user.get("email", ""),
                            "phone": None,  # Phone number is not provided in the response
                            "position_name": user.get("userRoleDetails", {})
                                            .get("primaryRoleDetails", {})
                                            .get("roleName", role),
                        })
            else:
                if response.get("status") == "Failed":
                    logger.error(response["message"]["reasons"])
                    break
                # Parse the response and filter for active users with the current role
                for user in response["data"]:
                    if user.get("role") == role and user.get("isActive"):
                        salespersons.append({
                            "crm_salesperson_id": user["id"],
                            "first_name": user["fname"],
                            "last_name": user["lname"],
                            "email": user["email"],
                            "phone": None,  # Phone number is not provided in the response
                            "position_name": user["role"]
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
