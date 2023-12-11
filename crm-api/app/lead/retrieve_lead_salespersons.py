"""Retrieve salespersons data from the shared CRM layer."""
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


def get_salespersons_from_crm(body: dict, partner_name: str) -> Any:
    """Get lead salespersons from CRM."""
    s3_key = f"configurations/{ENVIRONMENT}_{partner_name.upper()}.json"
    try:
        lambda_arn = loads(
                s3_client.get_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )["get_lead_salesperson_arn"]
    except Exception as e:
        logger.error(f"Failed to retrieve lambda ARN from S3 config. Partner: {partner_name.upper()}, {e}")
        raise

    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps(body),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode('utf-8'))
    if response_json["statusCode"] != 200:
        logger.error(f"Error retrieving salespersons {response_json['statusCode']}: {response_json}")
        raise
    return response_json


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve a list of all salespersons for a given lead id."""
    logger.info(f"Event: {event}")

    try:
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
            logger.info(f"lead: {lead.as_dict()}")

            crm_lead_id = lead.crm_lead_id
            dealer_integration_partner_id = lead.consumer.dealer_integration_partner.id
            crm_dealer_id = lead.consumer.dealer_integration_partner.crm_dealer_id
            partner_name = lead.consumer.dealer_integration_partner.integration_partner.impel_integration_partner_name

        payload = {
            "lead_id": lead_id,
            "dealer_integration_partner_id": dealer_integration_partner_id,
            "crm_lead_id": crm_lead_id,
            "crm_dealer_id": crm_dealer_id
        }
        try:
            response = get_salespersons_from_crm(payload, partner_name)
        except Exception as e:
            logger.error(f"Failed to retrieve lead salespersons from CRM. {e}")
            return {
                "statusCode": 202,
                "body": dumps({"message": "Accepted. The request was received but failed to be processed by the CRM"})
            }

        salespersons = []
        for person in loads(response["body"]).get("salespersons", []):
            salespersons.append(
                {
                    "crm_salesperson_id": person.get("crm_salesperson_id", ""),
                    "first_name": person.get("first_name", ""),
                    "last_name": person.get("last_name", ""),
                    "email": person.get("email", ""),
                    "phone": person.get("phone", ""),
                    "position_name": person.get("position_name", ""),
                    "is_primary": person.get("is_primary", False)
                }
            )

        if not salespersons:
            logger.error(f"No salespersons found for lead {lead_id}")
            return {
                "statusCode": 404,
                "body": dumps({"error": "No salespersons found for the given lead."})
            }

        return {
            "statusCode": 200,
            "body": dumps(salespersons)
        }

    except Exception as e:
        logger.exception(f"Error retrieving salespersons: {e}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
