
import logging
from os import environ
import boto3
from utils import call_crm_api
from json import dumps, loads

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
CRM_API_URL = environ.get("CRM_API_URL")
CRM_EVENT_BUS_NAME = environ.get("CRM_EVENT_BUS_NAME")

secret_client = boto3.client("secretsmanager")
logger = logging.getLogger()


def lambda_handler(event, context):
    """
    This function is triggered by a CRM API event.
    It processes the event, adds additional information
    and sends it to an CrmEventBus.
    """
    logger.info("Received event: %s", event)

    try:
        CRM_API_URL = 'https://wpl2oe1li7.execute-api.us-east-1.amazonaws.com/lucas2/'
        record = event["Records"][0]
        body = loads(record["body"])
        idp_dealer_id = body.get("Detail", {}).get("idp_dealer_id")

        dealer = call_crm_api(f"{CRM_API_URL}dealers/idp_dealer_id/{idp_dealer_id}")

        if not dealer:
            logger.error(f"Dealer not found for idp_dealer_id: {idp_dealer_id}")
            return {
                "statusCode": 404,
                "body": f"Dealer not found for idp_dealer_id: {idp_dealer_id}"
            }

        metadata = dealer.get("metadata") or {}
        oem_partner = metadata.get("oem_partner") or {}
        oem_partner_name = oem_partner.get("name")

        merged_event = {
            "detail": {
                **body,
                "event_type": [body.get("Detail", {}).get("event_type")],
                "source_application": [body.get("Detail", {}).get("source_application")],
                "partner_name": [dealer['integration_partner_name']],
                "override_partner": [f"{oem_partner_name}"] if oem_partner_name else [],
            }
        }

        logger.info(f"Event to be sent to EventBus: {merged_event}")

        event_bus_client = boto3.client('events')
        response = event_bus_client.put_events(
            Entries=[
                {
                    'Source': 'crm-api',
                    'DetailType': 'CRM API Event',
                    'Detail': dumps(merged_event),
                    'EventBusName': CRM_EVENT_BUS_NAME,
                },
            ]
        )
        logger.info(f"Event sent to EventBus: {response}")

        return {
            "statusCode": 200,
            "body": merged_event
        }

    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return {
            "statusCode": 500,
            "body": f"Error processing event: {e}"
        }
