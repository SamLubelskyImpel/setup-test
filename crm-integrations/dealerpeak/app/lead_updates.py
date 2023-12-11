"""Get lead updates from DealerPeak."""

import boto3
from json import dumps, loads
from os import environ
import logging
from base64 import b64encode
import requests

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")
sqs_client = boto3.client("sqs")
s3_client = boto3.client("s3")


def basic_auth(username: str, password: str) -> str:
    """Convert api crendentials to base64 encoded string."""
    token = b64encode(f"{username}:{password}".encode('ascii')).decode('ascii')
    return token


def get_secrets():
    """Get DealerPeak API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integration-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]


def get_lead(crm_dealer_id, crm_lead_id):
    """Get lead from DealerPeak."""
    api_url, username, password = get_secrets()
    token = basic_auth(username, password)

    dealer_group_id = crm_dealer_id.split("__")[0]

    try:
        response = requests.get(
            url=f"{api_url}/dealergroup/{dealer_group_id}/lead/{crm_lead_id}",
            headers={
                "Authorization": f"Basic {token}",
                "Content-Type": "application/json",
            },
            timeout=3,
        )
        logger.info(f"DealerPeak responded with: {response.status_code}")
        response.raise_for_status()
        return response.json()

    except Exception as e:
        logger.error(f"Error occured calling DealerPeak APIs: {e}")
        raise


def parse_salesperson(lead: dict):
    """Parse salesperson from lead."""
    agent = lead["agent"]
    user_id = agent["userID"]

    first_name = agent.get("givenName", "")
    last_name = agent.get("familyName", "")

    phones = agent["contactInformation"].get("phoneNumbers", [])
    emails = agent["contactInformation"].get("emails", [])

    phone_number = phones[0]["number"] if phones else ""
    email_address = emails[0]["address"] if emails else ""
    for phone in phones:
        if phone["type"].lower() in ("mobile", "cell"):
            phone_number = phone["number"]

    return {
        "crm_salesperson_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email_address,
        "phone": phone_number,
        "position_name": "Agent",
        "is_primary": True
    }


def send_sqs_message(message_body: dict):
    """Send SQS message."""
    s3_key = f"configurations/{ENVIRONMENT}_DEALERPEAK.json"
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


def lambda_handler(event, context):
    """Get lead updates."""
    logger.info(f"Event: {event}")
    try:
        lead_id = event["lead_id"]
        dealer_partner_id = event["dealer_integration_partner_id"]
        crm_lead_id = event["crm_lead_id"]
        crm_dealer_id = event["crm_dealer_id"]

        lead = get_lead(crm_dealer_id, crm_lead_id)
        if not lead:
            logger.info(f"Lead not found. lead_id {lead_id}, crm_lead_id {crm_lead_id}")
            return {
                "statusCode": 404,
                "body": dumps({
                    "error": f"Lead not found. lead_id {lead_id}, crm_lead_id {crm_lead_id}"
                })
            }

        salesperson = parse_salesperson(lead)
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

        return {
            "statusCode": 200,
            "body": dumps({
                "status": status,
                "salespersons": [salesperson]
            })
        }

    except Exception as e:
        logger.error(f"Error occured getting lead updates: {e}")
        return {
            "statusCode": 500,
            "error": "An error occurred while processing the request."
        }
