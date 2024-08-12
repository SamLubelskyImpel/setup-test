import boto3
import logging
from os import environ
from requests import post
from json import dumps, loads
from typing import Dict, Any, Tuple
from requests.auth import HTTPBasicAuth

# Configuration
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(environ.get("LOGLEVEL", "INFO").upper()))

# AWS clients
secret_client = boto3.client("secretsmanager")
sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def get_secrets() -> Tuple[str, str, str, str]:
    """Retrieve PBS API secrets."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    secret = secret_client.get_secret_value(SecretId=secret_id)
    secret_data = loads(secret["SecretString"])[SECRET_KEY]
    return (
        secret_data["API_URL"],
        secret_data["API_USERNAME"],
        secret_data["API_PASSWORD"],
        secret_data["SERIAL_NUMBER"],
    )


def get_lead(crm_dealer_id: str, crm_lead_id: str) -> Dict[str, Any]:
    """Fetch lead information from PBS."""
    url, username, password, serial_number = get_secrets()
    auth = HTTPBasicAuth(username, password)
    response = post(
        url=f"{url}/json/reply/DealContactVehicleGet",
        params={"SerialNumber": serial_number, "DealId": crm_lead_id},
        auth=auth,
    )
    logger.info("PBS responded with status code: %s", response.status_code)
    response.raise_for_status()
    return response.json()


def parse_salesperson(lead: Dict[str, Any]) -> list:
    """Extract salesperson details from lead information."""
    return [
        {
            "crm_salesperson_id": salesperson["EmployeeRef"],
            "first_name": salesperson["Name"].split(" ")[0],
            "last_name": salesperson["Name"].split(" ")[1] if len(salesperson["Name"].split(" ")) > 1 else "",
            "position_name": salesperson["Role"],
            "is_primary": salesperson["Primary"],
        }
        for salesperson in lead["DealUserRoles"]
    ]


def get_salesperson_by_lead_id(
    crm_dealer_id: str, crm_lead_id: str, lead_id: str, dealer_partner_id: str
) -> Tuple[int, Dict[str, Any]]:
    """Retrieve salesperson details by lead ID."""
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

    status_code, body = get_salesperson_by_lead_id(
        crm_dealer_id=crm_dealer_id,
        crm_lead_id=crm_lead_id,
        lead_id=lead_id,
        dealer_partner_id=dealer_partner_id,
    )

    return {"statusCode": status_code, "body": dumps(body)}
