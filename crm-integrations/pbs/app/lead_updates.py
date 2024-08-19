import boto3
import logging
from os import environ
from requests import post, put
from json import dumps, loads
from typing import Dict, Any, Tuple, Optional
from requests.auth import HTTPBasicAuth

# Configuration Constants
BUCKET = environ.get("INTEGRATIONS_BUCKET")
SECRET_KEY = environ.get("SECRET_KEY")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
UPLOAD_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

# Logging Configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(environ.get("LOGLEVEL", "INFO").upper()))

# AWS Clients
secret_client = boto3.client("secretsmanager")
sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def get_secret(secret_name: str, secret_key: Optional[str] = None) -> Any:
    """Retrieve a secret or specific key from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    secret = secret_client.get_secret_value(SecretId=secret_id)
    secret_data = loads(secret["SecretString"])
    return secret_data if secret_key is None else secret_data.get(secret_key)


def get_lead(crm_dealer_id: str, crm_lead_id: str) -> Dict[str, Any]:
    """Fetch lead information from PBS."""
    secret_data = get_secret("crm-integrations-partner")
    url = secret_data["API_URL"]
    username = secret_data["API_USERNAME"]
    password = secret_data["API_PASSWORD"]
    
    auth = HTTPBasicAuth(username, password)
    response = post(
        url=f"{url}/json/reply/DealContactVehicleGet",
        params={"SerialNumber": crm_dealer_id, "DealId": crm_lead_id},
        auth=auth,
    )
    logger.info("PBS responded with status code: %s", response.status_code)
    response.raise_for_status()
    return response.json()


def update_lead_data(lead_id: str, data: Dict[str, Any], crm_api_key: str) -> Any:
    """Update lead status through CRM API."""
    url = f"https://{CRM_API_DOMAIN}/leads/{lead_id}"
    headers = {
        "partner_id": UPLOAD_SECRET_KEY,
        "x_api_key": crm_api_key
    }
    response = put(url, headers=headers, json=data)
    logger.info("CRM API Put Lead responded with: %s", response.status_code)
    response.raise_for_status()
    return response.json()


def parse_sp_name(salesperson_name: str) -> Tuple[str, str]:
    """Parse the salesperson's name into first and last name."""
    try:
        first_name, last_name = salesperson_name.split()
    except ValueError:
        logger.warning("Unexpected salesperson name format: %s", salesperson_name)
        first_name = salesperson_name.strip().replace(" ", "")
        last_name = ""
    return first_name, last_name


def parse_salesperson(lead: Dict[str, Any]) -> list:
    """Extract salesperson details from lead information."""
    return [
        {
            "crm_salesperson_id": sp["EmployeeRef"],
            "first_name": parse_sp_name(sp["Name"])[0],
            "last_name": parse_sp_name(sp["Name"])[1],
            "position_name": sp["Role"],
            "is_primary": True,
        }
        for sp in lead.get("DealUserRoles", [])
    ]


def get_lead_update(
    crm_dealer_id: str, crm_lead_id: str, lead_id: str, dealer_partner_id: str
) -> Tuple[int, Dict[str, Any]]:
    """Retrieve and update lead information."""
    try:
        lead = get_lead(crm_dealer_id, crm_lead_id)
    except Exception as e:
        logger.error("Error occurred while calling PBS APIs: %s", e)
        logger.error(
            "[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] "
            "DealerIntegrationPartnerId: %s, LeadId: %s, CrmDealerId: %s, CrmLeadId: %s",
            dealer_partner_id, lead_id, crm_dealer_id, crm_lead_id
        )
        raise

    if not lead.get("Items"):
        logger.info("Lead not found. lead_id: %s, crm_lead_id: %s", lead_id, crm_lead_id)
        return 404, {"error": f"Lead not found. lead_id: {lead_id}, crm_lead_id: {crm_lead_id}"}

    lead_item = lead["Items"][0]
    salesperson = parse_salesperson(lead_item)
    status = lead_item["DealStatus"]

    crm_api_key = get_secret("crm-api", UPLOAD_SECRET_KEY)["api_key"]
    update_lead_data(
        lead_id=lead_id,
        data={
            "lead_status": status,
            "salespersons": salesperson
        },
        crm_api_key=crm_api_key
    )

    logger.info(
        "Found lead %s, dealer_integration_partner %s, with status %s and salesperson %s",
        lead_id, dealer_partner_id, status, salesperson
    )

    return 200, {"status": status, "salespersons": salesperson}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda function handler for retrieving lead updates."""
    logger.info("Received event: %s", event)

    crm_dealer_id = event["crm_dealer_id"]
    dealer_partner_id = event.get("dealer_integration_partner_id", "")
    lead_id = event.get("lead_id", "")
    crm_lead_id = event.get("crm_lead_id", "")

    status_code, body = get_lead_update(
        crm_dealer_id=crm_dealer_id,
        crm_lead_id=crm_lead_id,
        lead_id=lead_id,
        dealer_partner_id=dealer_partner_id,
    )

    return {"statusCode": status_code, "body": dumps(body)}
