import logging
from datetime import datetime, timezone
from json import dumps, loads
from os import environ
from typing import Any, Dict, Optional
from uuid import uuid4

import boto3
from dataclasses import dataclass, asdict
from requests import get

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
        res["body"] = dumps(res["body"])
        return res


def get_tekion_secrets():
    """Get Tekion API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)
    token = loads(
        s3_client.get_object(Bucket=BUCKET, Key=f"tekion_crm/token.json")["Body"]
        .read()
        .decode("utf-8")
    )
    return secret_data["url"], secret_data["app_id"], token["token"]


def save_raw_response(response, dealer_id):
    """Save raw response to S3."""
    now = datetime.now(timezone.utc)
    s3_key = f"raw_updates/tekion/{dealer_id}/{now.year}/{now.month}/{now.day}/{now.hour}/{str(uuid4())}.json"
    s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=response)
    logger.info(f"Saved raw response to S3: {s3_key}")


def call_tekion_api(endpoint: str, dealer_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Call Tekion API."""
    api_url, client_id, access_key = get_tekion_secrets()
    headers = {
        "Content-Type": "application/json",
        "dealer_id": dealer_id,
        "app_id": client_id,
        "Authorization": f"Bearer {access_key}",
    }
    response = get(
        url=f"{api_url}/{endpoint}",
        headers=headers,
        params=params,
    )

    logger.info(f"Tekion responded with: {response.status_code}, {response.text}")
    save_raw_response(response.text, dealer_id)
    response.raise_for_status()
    return response.json()


def parse_salesperson(assignees: list):
    """Parse salesperson."""
    primary_assignee = None
    for assignee in assignees:
        if (
            assignee.get("isPrimary", False)
            and assignee.get("type", "") == "SALES_PERSON"
        ):
            primary_assignee = assignee
            break
    else:
        primary_assignee = assignees[0] if assignees else {}

    return {
        "crm_salesperson_id": primary_assignee.get("id"),
        "first_name": "",
        "last_name": "",
        "position_name": primary_assignee.get("role"),
        "is_primary": bool(primary_assignee.get("isPrimary"))
    }


def send_sqs_message(message_body: Dict[str, Any]) -> None:
    """Send SQS message."""
    s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_TEKION.json"
    try:
        queue_url = loads(
            s3_client.get_object(Bucket=BUCKET, Key=s3_key)["Body"]
            .read()
            .decode("utf-8")
        )["lead_updates_queue_url"]

        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=dumps(message_body)
        )
    except Exception:
        logger.exception("Error sending SQS message")


def get_lead_salesperson_update_from_crm(
    crm_dealer_id, crm_lead_id, lead_id, dealer_partner_id
) -> ApiResponse:
    """Get lead from Tekion."""
    tekion_res = call_tekion_api(
        endpoint=f"openapi/v4.0.0/leads/{crm_lead_id}/assignees",
        dealer_id=crm_dealer_id
    )
    lead_assignees = tekion_res.get("data", [])

    if not lead_assignees:
        logger.info(
            f"Lead Assignees not found. " f"lead_id={lead_id}, " f"crm_lead_id={crm_lead_id}"
        )
        body = {
            "error": f"Lead Assignees not found. " f"lead_id={lead_id}, " f"crm_lead_id={crm_lead_id}"
        }
        return ApiResponse(404, body)

    salesperson = parse_salesperson(lead_assignees)

    logger.info(
        f"Found lead_id={lead_id}, "
        f"dealer_integration_partner={dealer_partner_id}, "
        f"salesperson={salesperson}"
    )

    send_sqs_message(
        {
            "lead_id": lead_id,
            "dealer_integration_partner_id": dealer_partner_id,
            "salespersons": [salesperson],
        }
    )

    body = {"salespersons": [salesperson]}
    return ApiResponse(200, body)


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Get lead updates."""
    logger.info(f"Event: {event}")

    crm_dealer_id = event["crm_dealer_id"]
    dealer_partner_id = event.get("dealer_integration_partner_id")
    lead_id = event.get("lead_id")
    crm_lead_id = event.get("crm_lead_id")

    try:
        res = get_lead_salesperson_update_from_crm(
            crm_dealer_id=crm_dealer_id,
            crm_lead_id=crm_lead_id,
            lead_id=lead_id,
            dealer_partner_id=dealer_partner_id,
        )
        return res.to_json()
    except Exception:
        logger.exception(
            "[SUPPORT ALERT] Failed to Get Lead Salesperson Update [CONTENT] "
            "DealerIntegrationPartnerId: %s LeadId: %s CrmDealerId: %s CrmLeadId: %s",
            dealer_partner_id,
            lead_id,
            crm_dealer_id,
            crm_lead_id,
        )
        raise
