from typing import Any, Dict
import logging
from os import environ
from json import dumps, loads
import boto3
from requests import get
from datetime import datetime
from uuid import uuid4
from dataclasses import dataclass, asdict


ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


@dataclass
class ApiResponse:
    """Wrapper for API response."""

    statusCode: int
    body: Dict[str, Any]

    def to_json(self):
        res = asdict(self)
        res['body'] = dumps(res['body'])
        return res

def get_tekion_secrets():
    """Get Tekion API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    token = loads(s3_client.get_object(Bucket=BUCKET, Key=f"tekion_crm/token.json")["Body"].read().decode('utf-8'))

    return secret_data["url"], secret_data["app_id"], token["token"]


def save_raw_response(response, dealer_id):
    """Save raw response to S3."""
    now = datetime.utcnow()
    s3_key = f"raw_updates/tekion/{dealer_id}/{now.year}/{now.month}/{now.day}/{now.hour}/{str(uuid4())}.json"
    s3_client.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=response
    )
    logger.info(f"Saved raw response to S3: {s3_key}")


def hit_tekion_api(endpoint, params, dealer_id):
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
    save_raw_response(response.text, dealer_id)
    response.raise_for_status()
    return response.json()


def parse_salesperson(assignee: dict):
    return {
        "crm_salesperson_id": assignee.get("arcId"),
        "first_name": assignee.get("firstName"),
        "last_name": assignee.get("lastName"),
        "position_name": assignee.get("type"),
    }


def send_sqs_message(message_body: Dict[str, Any]) -> None:
    """Send SQS message."""
    s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_TEKION.json"
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
    except Exception:
        logger.exception("Error sending SQS message")


def get_lead_update_from_crm(crm_dealer_id, crm_lead_id, lead_id, dealer_partner_id) -> ApiResponse:
    """Get lead from Tekion."""

    tekion_res = hit_tekion_api("openapi/v3.1.0/crm-leads", { "id": crm_lead_id }, crm_dealer_id)
    lead = next(iter(tekion_res["data"]), None)

    if not lead:
        logger.info(f"Lead not found. "
                    f"lead_id={lead_id}, "
                    f"crm_lead_id={crm_lead_id}")
        body = { "error": f"Lead not found. lead_id={lead_id}, crm_lead_id={crm_lead_id}" }
        return ApiResponse(404, body)

    salesperson = parse_salesperson(lead["assignees"][0])
    status = lead.get("status")

    logger.info(
        f"Found lead_id={lead_id}, "
        f"dealer_integration_partner={dealer_partner_id}, "
        f"status={status} "
        f"salesperson={salesperson}")

    send_sqs_message({
        "lead_id": lead_id,
        "dealer_integration_partner_id": dealer_partner_id,
        "status": status,
        "salespersons": [salesperson]
    })

    body = { "status": status, "salespersons": [salesperson] }
    return ApiResponse(200, body)


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Get lead updates."""
    logger.info(f"Event: {event}")

    crm_dealer_id = event["crm_dealer_id"]
    dealer_partner_id = event.get("dealer_integration_partner_id")
    lead_id = event.get("lead_id")
    crm_lead_id = event.get("crm_lead_id")

    try:
        res = get_lead_update_from_crm(crm_dealer_id=crm_dealer_id, crm_lead_id=crm_lead_id, lead_id=lead_id, dealer_partner_id=dealer_partner_id)
        return res.to_json()
    except Exception:
        logger.exception(
            f"[SUPPORT ALERT] Failed to Get Lead Update\n"
            f"[CONTENT] DealerIntegrationPartnerId: {dealer_partner_id}\n"
            f"LeadId: {lead_id}\n"
            f"CrmDealerId: {crm_dealer_id}\n"
            f"CrmLeadId: {crm_lead_id}")
        raise

