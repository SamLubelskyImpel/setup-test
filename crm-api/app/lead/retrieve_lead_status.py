"""Retrieve lead status from the shared CRM layer."""
import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any

from crm_orm.models.lead import Lead
from crm_orm.session_config import DBSession

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")


def get_status_from_crm(body: dict, partner_name: str) -> Any:
    """Get lead status from CRM."""
    lambda_arn = loads(
            s3_client.get_object(
                Bucket=BUCKET,
                Key=f"configurations/{ENVIRONMENT}_{partner_name.upper()}.json"
            )
        )["get_lead_status_arn"]

    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps(body),
    )
    logger.info(f"Response from lambda: {response}")
    return response["Payload"].read()


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve lead status."""
    try:
        logger.info(f"Event: {event}")

        lead_id = event["pathParameters"]["lead_id"]

        with DBSession() as session:
            lead = session.query(
                Lead
            ).filter(
                Lead.id == lead_id
            ).first()

            if not lead:
                logger.error(f"Lead not found {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead not found {lead_id}"})
                }

            crm_lead_id = lead.crm_lead_id
            dealer_id = lead.consumer.dealer.id
            crm_dealer_id = lead.consumer.dealer.crm_dealer_id
            partner_name = lead.consumer.dealer.integration_partner.impel_integration_partner_name

        payload = {
            "lead_id": lead_id,
            "crm_lead_id": crm_lead_id,
            "dealer_id": dealer_id,
            "crm_dealer_id": crm_dealer_id
        }
        try:
            response = get_status_from_crm(payload, partner_name)
        except Exception as e:
            logger.error(f"Failed to retrieve lead status from CRM. {e}")
            raise

        if response["statusCode"] != 200:
            logger.error(f"Error retrieving lead status {response['statusCode']}: {response['body']}")
            return {
                "statusCode": 202,
                "body": dumps({"message": "Accepted. The request was received by failed to be processed by the CRM"})
            }

        status = loads(response["body"]).get("status", "")
        if not status:
            logger.error(f"No lead status found for lead {lead_id}")
            return {
                "statusCode": 404,
                "body": dumps({"error": "No status found for the given lead."})
            }

        return {
            "statusCode": 200,
            "body": dumps({"lead_status": status})
        }

    except Exception as e:
        logger.exception(f"Error retrieving lead status: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
