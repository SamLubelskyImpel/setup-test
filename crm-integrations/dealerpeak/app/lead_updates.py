"""Get lead updates from DealerPeak."""

import boto3
import logging
from os import environ
from requests import get
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
    """Get DealerPeak API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]


def get_lead(crm_dealer_id: str, crm_lead_id: str) -> Dict[str, Any]:
    """Get lead from DealerPeak."""
    api_url, username, password = get_secrets()
    auth = HTTPBasicAuth(username, password)
    dealer_group_id = crm_dealer_id.split("__")[0]
    response = get(
        url=f"{api_url}/dealergroup/{dealer_group_id}/lead/{crm_lead_id}",
        auth=auth,
        timeout=3,
    )
    response.raise_for_status()
    return response.json()


def parse_salesperson(agent: dict):
    """Parse salesperson from lead."""
    user_id = agent["userID"]

    first_name = agent.get("givenName", "")
    last_name = agent.get("familyName", "")

    phones = agent["contactInformation"].get("phoneNumbers", [{}])
    emails = agent["contactInformation"].get("emails", [{}])

    phone_number = phones[0].get("number") if phones else ""
    email_address = emails[0].get("address") if emails else ""
    for phone in phones:
        if phone["type"].lower() in ("mobile", "cell"):
            phone_number = phone.get("number")

    return {
        "crm_salesperson_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email_address,
        "phone": phone_number,
        "position_name": "Agent",
        "is_primary": True
    }


def send_sqs_message(message_body: Dict[str, Any]) -> None:
    """Send SQS message."""
    s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_DEALERPEAK.json"
    try:
        queue_url = loads(
            s3_client.get_object(
                Bucket=BUCKET,
                Key=s3_key
            )['Body'].read().decode('utf-8')
        )["lead_updates_queue_url"]

        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(message_body)
        )
    except Exception as e:
        logger.error(f"Error sending SQS message: {e}")


def get_salesperson_by_lead_id(crm_dealer_id: str, crm_lead_id: str, lead_id: str, dealer_partner_id: str) -> Tuple[str, Dict[str, Any]]:
    try:
        lead = get_lead(crm_dealer_id, crm_lead_id)
    except Exception as e:
        logger.error(f"Error occurred calling DealerPeak APIs: {e}")
        logger.error("[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] DealerIntegrationPartnerId: {}\nLeadId: {}\nCrmDealerId: {}\nCrmLeadId: {}\nTraceback: {}".format(
            dealer_partner_id, lead_id, crm_dealer_id, crm_lead_id, e)
            )
        raise

    if not lead:
        logger.info(f"Lead not found. lead_id {lead_id}, crm_lead_id {crm_lead_id}")
        return (
            404,
            {
                "error": f"Lead not found. lead_id {lead_id}, crm_lead_id {crm_lead_id}"
            }
        )

    salesperson = parse_salesperson(lead["agent"])
    status = lead["status"].get("status", "")

    logger.info("Found lead {}, dealer_integration_partner {}, with status {} and salesperson {}".format(
        lead_id, dealer_partner_id, status, salesperson
    ))
    send_sqs_message({
        "lead_id": lead_id,
        "dealer_integration_partner_id": dealer_partner_id,
        "status": status,
        "salespersons": [salesperson]
    })

    return status, salesperson


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Get lead updates."""
    logger.info(f"Event: {event}")

    crm_dealer_id = event["crm_dealer_id"]

    dealer_partner_id = event.get("dealer_integration_partner_id")
    lead_id = event.get("lead_id")
    crm_lead_id = event.get("crm_lead_id")

    if lead_id and crm_lead_id:
        status, salesperson = get_salesperson_by_lead_id(crm_dealer_id=crm_dealer_id, crm_lead_id=crm_lead_id, lead_id=lead_id, dealer_partner_id=dealer_partner_id)

        return {
            "statusCode": 200,
            "body": dumps({
                "status": status,
                "salespersons": [salesperson]
            })
        }
    else:
        api_url, username, password = get_secrets()
        auth = HTTPBasicAuth(username, password)
        dealer_group_id, location_id = crm_dealer_id.split("__")

        response = get(    
            url=f"{api_url}/dealergroup/{dealer_group_id}/location/{location_id}/employees",
            auth=auth,
            timeout=3,
        )
        response.raise_for_status()

        salespersons = [
            parse_salesperson(x) for x in response.json()
        ]

        return {
            "statusCode": 200,
            "body": dumps(salespersons)
        }
