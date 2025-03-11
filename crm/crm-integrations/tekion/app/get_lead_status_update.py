import logging
from datetime import datetime, timezone
from json import dumps, loads
from os import environ
from typing import Any, Dict, Optional
from uuid import uuid4

import boto3
from dataclasses import dataclass, asdict
from requests import get, RequestException

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")
CONFIG_FILE_KEY = "configurations/tekion_api_version_config.json"
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN", "crm-api-test.testenv.impel.io")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY", "impel")

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


def get_secret(secret_id: str) -> Optional[Dict]:
    """Fetch and parse secret from AWS Secrets Manager."""
    try:
        secret = secret_client.get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except secret_client.exceptions.ResourceNotFoundException:
        logger.error("Secret not found")
    except Exception:
        logger.exception("Failed to retrieve secret")
    return None


def get_tekion_secrets():
    """Get Tekion API secrets."""
    secret_response = get_secret(f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner")
    secret_data = loads(secret_response[str(SECRET_KEY)])
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

    response_json = response.json()

    return response_json


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


def get_api_version_config():
    """Retrieve API version configuration from S3."""
    try:
        response = s3_client.get_object(Bucket=BUCKET, Key=CONFIG_FILE_KEY)
        content = response['Body'].read().decode('utf-8')
        return loads(content)
    except Exception as e:
        logger.error(f"Failed to fetch API version config: {e}")
        return {}


def get_lead_status_update_from_crm(crm_dealer_id, crm_lead_id, lead_id, dealer_partner_id) -> ApiResponse:
    """Get lead from Tekion."""
    
    version_config = get_api_version_config()
    product_dealer_id = get_product_dealer_id(crm_dealer_id)
    api_version = version_config.get(product_dealer_id, "v3")

    tekion_res = call_tekion_api(
        endpoint=(
            f"openapi/v4.0.0/leads/{crm_lead_id}"
            if api_version == "v4"
            else "openapi/v3.1.0/crm-leads"
        ),
        dealer_id=crm_dealer_id,
        params={"id": crm_lead_id} if api_version == "v3" else None,
    )

    if api_version == "v3":
        lead = next(iter(tekion_res["data"]), None)
        if not lead:
            logger.info(f"Lead not found. "
            f"lead_id={lead_id}, "
            f"crm_lead_id={crm_lead_id}")
            body = {"error": f"Lead not found. lead_id={lead_id}, crm_lead_id={crm_lead_id}"}
            return ApiResponse(404, body)
        status = lead.get("status")
    else:
        if tekion_res.get("meta", {}).get("status", "").lower() != "success":
            logger.info(f"Lead not found. "
                        f"lead_id={lead_id}, "
                        f"crm_lead_id={crm_lead_id}")
            body = {"error": f"Lead not found. lead_id={lead_id}, crm_lead_id={crm_lead_id}"}
            return ApiResponse(404, body)

        status = tekion_res['data'].get("status", "")

    logger.info(
        f"Found lead_id={lead_id}, "
        f"dealer_integration_partner={dealer_partner_id}, "
        f"status={status}"
    )

    send_sqs_message(
        {
            "lead_id": lead_id,
            "dealer_integration_partner_id": dealer_partner_id,
            "status": status
        }
    )
    body = {"status": status}
    return ApiResponse(200, body)


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Get lead updates."""
    logger.info(f"Event: {event}")

    crm_dealer_id = event["crm_dealer_id"]
    dealer_partner_id = event.get("dealer_integration_partner_id")
    lead_id = event.get("lead_id")
    crm_lead_id = event.get("crm_lead_id")

    try:
        res = get_lead_status_update_from_crm(
            crm_dealer_id=crm_dealer_id,
            crm_lead_id=crm_lead_id,
            lead_id=lead_id,
            dealer_partner_id=dealer_partner_id,
        )
        return res.to_json()
    except Exception:
        logger.exception(
            "[SUPPORT ALERT] Failed to Get Lead Status Update [CONTENT] "
            "DealerIntegrationPartnerId: %s LeadId: %s CrmDealerId: %s CrmLeadId: %s",
            dealer_partner_id,
            lead_id,
            crm_dealer_id,
            crm_lead_id,
        )
        raise

