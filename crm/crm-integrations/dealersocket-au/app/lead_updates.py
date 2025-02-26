import boto3
import logging
from json import dumps, loads
from os import environ
from typing import Any, Dict
from dealer_socket_client import DealerSocketClient, DS_LEAD_STATUS_MAPPINGS

ENVIRONMENT = environ.get("ENVIRONMENT")
DEALERSOCKET_VENDOR = environ.get("DEALERSOCKET_VENDOR")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def send_sqs_message(message_body: Dict[str, Any]) -> None:
    """Send SQS message."""
    s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_DEALERSOCKET_AU.json"
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


def search_lead(crm_dealer_id: str, crm_consumer_id: str) -> Dict[str, Any]:
    """Search lead in CRM."""
    dealersocket_client = DealerSocketClient()
    lead_response = dealersocket_client.query_event(DEALERSOCKET_VENDOR, crm_dealer_id, crm_consumer_id)
    if not lead_response:
        raise Exception("No lead returned by DealerSocket AU")

    logger.info(f"Dealersocket Lead API Response: {lead_response}")

    return lead_response


def get_lead_updates(crm_dealer_id: str, crm_consumer_id: str, crm_lead_id: str, dealer_partner_id: str, lead_id: str) -> Dict[str, Any]:
    """Get lead updates from CRM."""
    event_response = search_lead(crm_dealer_id, crm_consumer_id)

    events = event_response.get("events", [])
    for event in events:
        if event.get("eventId") == crm_lead_id:
            lead = event
            break
    else:
        raise Exception("Lead not found in events")

    if lead.get("primaryAssigned"):
        salespersons = [{
            "crm_salesperson_id": lead["primaryAssigned"],
            "is_primary": True
        }]
    else:
        salespersons = []

    lead_status = int(lead.get("status")) if lead.get("status") else None  # type: ignore
    if not lead_status:
        status = ""
    else:
        status = DS_LEAD_STATUS_MAPPINGS.get(lead_status)  # type: ignore
        if not status:
            raise Exception(f"Invalid status: {lead_status}")

    send_sqs_message({
        "lead_id": lead_id,
        "dealer_integration_partner_id": dealer_partner_id,
        "status": status,
        "salespersons": salespersons
    })

    return {"status": status, "salespersons": salespersons}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Get lead updates."""
    logger.info(f"Event: {event}")

    try:
        crm_dealer_id = event.get("crm_dealer_id")
        dealer_partner_id = event.get("dealer_integration_partner_id")
        lead_id = event.get("lead_id")
        crm_lead_id = event.get("crm_lead_id")
        crm_consumer_id = event.get("crm_consumer_id")

        response = get_lead_updates(crm_dealer_id, crm_consumer_id, crm_lead_id, dealer_partner_id, lead_id)  # type: ignore
        logger.info(f"Response: {response}")

        return {
            "statusCode": 200,
            "body": dumps(response)
        }
    except Exception:
        logger.exception("[SUPPORT ALERT] Failed to Get Lead Update [CONTENT] DealerIntegrationPartnerId: {} LeadId: {} CrmDealerId: {} CrmLeadId: {}".format(
            dealer_partner_id, lead_id, crm_dealer_id, crm_lead_id
        ))
        raise
