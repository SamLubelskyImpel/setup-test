import logging
from utils import call_crm_api
from os import environ
from da_event_helper import create_webhook_payload, send_to_webhook

CRM_API_URL = environ.get("CRM_API_URL")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_lead_by_lead_id(lead_id):
    lead_information = call_crm_api(f"{CRM_API_URL}leads/{lead_id}")

    if "consumer_id" not in lead_information:
        logger.error(f"ERROR [get_lead_by_lead_id] lead_id={lead_id} has no consumer_id.")

        return lead_information, {"error": "Consumer_id not in lead_information"}

    consumer_information = call_crm_api(f"{CRM_API_URL}consumers/{lead_information['consumer_id']}")

    return lead_information, consumer_information


def lambda_handler(event: dict, context: dict) -> None:
    try:
        logger.info(f"This is the event body: {event}")
        lead_inf, consumer_inf = get_lead_by_lead_id(event.get("lead_id"))

        if lead_inf.get("error") or consumer_inf.get("error"):
            logger.error(f"[new_lead_events_handler] error occurred getting lead information: \n {lead_inf.get('error')}")

        lead_inf["lead_id"] = event.get("lead_id")
        webhook_payload = create_webhook_payload(lead_inf, consumer_inf)
        logger.info(f"This is the webhook payload: {webhook_payload}")

        if webhook_payload:
            status_code = send_to_webhook(webhook_payload)
            if status_code == 200:
                logger.info("INFO [new_lead_events_handler] webhook request successful")
            else:
                logger.error("[new_lead_events_handler] webhook request failed")

    except Exception as e:
        logger.error(f"[new_lead_events_handler] error occurred processing event listener: \n {e}")
