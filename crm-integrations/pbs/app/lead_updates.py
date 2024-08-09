"""Get lead updates from PBS."""

import boto3
import logging
from os import environ
from requests import post
from json import dumps, loads
from typing import Dict, Any, Tuple
from requests.auth import HTTPBasicAuth

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

secret_client = boto3.client("secretsmanager")
sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def get_secrets():
    """Get PBS API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    return (
        secret_data["API_URL"],
        secret_data["API_USERNAME"],
        secret_data["API_PASSWORD"],
        secret_data["SERIAL_NUMBER"],
    )
    # {"API_URL":"https://partnerhub.pbsdealers.com", "API_USERNAME":"Impel","API_PASSWORD":"fV4*eW39!#","SERIAL_NUMBER":"2004.QA"}


def get_lead(crm_dealer_id, crm_lead_id):
    """Get lead from PBS."""
    url, username, password, serial_number = get_secrets()
    auth = HTTPBasicAuth(username, password)
    response = post(
        url=f"{url}/json/reply/DealContactVehicleGet",
        params={"SerialNumber": serial_number, "DealerId": crm_lead_id},
        auth=auth,
    )
    logger.info(f"PBS responded with: {response.status_code}")
    response.raise_for_status()
    return response.json()


def parse_salesperson(lead):
    """Parse salesperson from lead."""
    salespersons_list = lead["DealUserRoles"]

    return [
        {
            "crm_salesperson_id": salesperson["EmployeeRef"],
            "first_name": salesperson["Name"].split(" ")[0],
            "last_name": salesperson["Name"].split(" ")[0],
            "position_name": salesperson["Role"],
            "is_primary": salesperson["Primary"],
        }
        for salesperson in salespersons_list
    ]


def get_salesperson_by_lead_id(
    crm_dealer_id: str, crm_lead_id: str, lead_id: str, dealer_partner_id: str
) -> Tuple[str, Dict[str, Any]]:
    try:
        lead = get_lead(crm_dealer_id, crm_lead_id)
    except Exception as e:
        logger.error(f"Error occurred calling PBS APIs: {e}")
        logger.error(
            "[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nCrmDealerId: {}\nCrmLeadId: {}\nTraceback: {}".format(
                dealer_partner_id, lead_id, crm_dealer_id, crm_lead_id, e
            )
        )
        raise

    if not lead.get("Items"):
        logger.info(f"Lead not found. lead_id {lead_id}, crm_lead_id {crm_lead_id}")
        return (
            404,
            {"error": f"Lead not found. lead_id {lead_id}, crm_lead_id {crm_lead_id}"},
        )

    salesperson = parse_salesperson(lead["Items"][0])
    status = lead["Items"][0]["DealStatus"]

    logger.info(
        "Found lead {}, dealer_integration_partner {}, with status {} and salesperson {}".format(
            lead_id, dealer_partner_id, status, salesperson
        )
    )

    return (200, {"status": status, "salespersons": [salesperson]})


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Get lead updates."""
    logger.info(f"Event: {event}")

    crm_dealer_id = event["crm_dealer_id"]

    dealer_partner_id = event.get("dealer_integration_partner_id")
    lead_id = event.get("lead_id")
    crm_lead_id = event.get("crm_lead_id")

    statusCode, body = get_salesperson_by_lead_id(
        crm_dealer_id=crm_dealer_id,
        crm_lead_id=crm_lead_id,
        lead_id=lead_id,
        dealer_partner_id=dealer_partner_id,
    )
    return {"statusCode": statusCode, "body": dumps(body)}