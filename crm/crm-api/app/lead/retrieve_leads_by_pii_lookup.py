"""Retrieve leads from the CRM by querying consumer PII."""
import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any

from crm_orm.models.dealer import Dealer
from crm_orm.session_config import DBSession
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")


def get_lambda_arn(partner_name: str) -> Any:
    """Get lambda ARN from S3."""
    s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_{partner_name.upper()}.json"
    s3_object = loads(
            s3_client.get_object(
                Bucket=BUCKET,
                Key=s3_key
            )['Body'].read().decode('utf-8')
        )
    lambda_arn = s3_object.get("get_leads_by_pii_arn")
    return lambda_arn


def get_leads_from_crm(payload: dict, lambda_arn: str) -> Any:
    """Get lead status from CRM."""
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps(payload),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode('utf-8'))
    if response_json["statusCode"] != 200:
        logger.error(f"Error retrieving leads {response_json['statusCode']}: {response_json}")
        raise Exception(f"CRM Lambda Failed during execution. Lambda Response: {response_json}")

    data = loads(response_json["body"])
    return data


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve leads from specified Consumer PII."""
    try:
        logger.info(f"Event: {event}")

        query_params = event["queryStringParameters"]

        dealer_id = query_params["dealer_id"]

        with DBSession() as session:
            db_results = session.query(
                Dealer, DealerIntegrationPartner.crm_dealer_id, IntegrationPartner.impel_integration_partner_name
            ).join(
                DealerIntegrationPartner, DealerIntegrationPartner.dealer_id == Dealer.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Dealer.product_dealer_id == dealer_id
            ).first()

            if not db_results:
                logger.error(f"Dealer not found {dealer_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Dealer not found {dealer_id}"})
                }

            dealer_db, crm_dealer_id, partner_name = db_results

            logger.info(f"Dealer: {dealer_db.as_dict()}")

        payload = {
            "crm_dealer_id": crm_dealer_id,
            "customer_info": dumps({
                "first_name": query_params.get("first_name"),
                "last_name": query_params.get("last_name"),
                "email": query_params.get("email"),
                "phone": query_params.get("phone")
            })
        }

        logger.info(f"Payload to Integration: {dumps(payload)}")

        lambda_arn = get_lambda_arn(partner_name)
        if lambda_arn:
            logger.info(f"Lambda ARN detected: {lambda_arn}. Retrieving leads from CRM.")
            try:
                leads = get_leads_from_crm(payload, lambda_arn)
            except Exception:
                logger.exception("Failed to retrieve leads from CRM.")
                return {
                    "statusCode": 202,
                    "body": dumps({"message": "Accepted. The request was received but failed to be processed by the CRM."})
                }
        else:
            logger.warning(f"No lambda ARN detected for partner {partner_name}. Cannot submit query.")
            return {
                "statusCode": 404,
                "body": dumps({"error": "Integration Partner is not configured for this function."})
            }

        return {
            "statusCode": 200,
            "body": dumps(leads)
        }

    except Exception:
        logger.exception("Error retrieving leads.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
