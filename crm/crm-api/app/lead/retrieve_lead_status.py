"""Retrieve lead status from the shared CRM layer."""
import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
import botocore.exceptions

from crm_orm.models.lead import Lead
from crm_orm.session_config import DBSession
from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner
from crm_orm.models.consumer import Consumer

ENVIRONMENT = environ.get("ENVIRONMENT")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")


def get_lambda_arn(partner_name: str) -> Any:
    """Get lambda ARN from S3."""
    s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_{partner_name.upper()}.json"
    try:
        s3_object = loads(
                s3_client.get_object(
                    Bucket=BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )
        lambda_arn = s3_object.get("get_lead_status_arn")
        return lambda_arn

    except botocore.exceptions.ClientError as e:
        logger.error(f"Error retrieving configuration file for {partner_name}")
        raise Exception(e)

    except Exception as e:
        logger.error(f"Failed to retrieve lambda ARN from S3 config. Partner: {partner_name.upper()}, {e}")
        raise Exception(e)


def get_status_from_crm(body: dict, lambda_arn: str) -> Any:
    """Get lead status from CRM."""
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

    status = loads(response_json["body"]).get("status", "")
    return status


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve lead status."""
    try:
        logger.info(f"Event: {event}")

        lead_id = event["pathParameters"]["lead_id"]

        with DBSession() as session:
            db_results = session.query(
                Lead, Consumer.crm_consumer_id, DealerIntegrationPartner, IntegrationPartner.impel_integration_partner_name
            ).join(
                Consumer, Lead.consumer_id == Consumer.id
            ).join(
                DealerIntegrationPartner, Consumer.dealer_integration_partner_id == DealerIntegrationPartner.id
            ).join(
                IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
            ).filter(
                Lead.id == lead_id
            ).first()

            if not db_results:
                logger.error(f"Lead not found {lead_id}")
                return {
                    "statusCode": 404,
                    "body": dumps({"error": f"Lead not found {lead_id}"})
                }

            lead_db, crm_consumer_id, dip_db, partner_name = db_results

            logger.info(f"lead: {lead_db.as_dict()}")

        payload = {
            "lead_id": lead_id,
            "dealer_integration_partner_id": dip_db.id,
            "crm_lead_id": lead_db.crm_lead_id,
            "crm_dealer_id": dip_db.crm_dealer_id,
            "crm_consumer_id": crm_consumer_id
        }

        lambda_arn = get_lambda_arn(partner_name)
        if lambda_arn:
            logger.info(f"Lambda ARN detected for partner {partner_name}. Retrieving status from CRM.")
            try:
                status = get_status_from_crm(payload, lambda_arn)
            except Exception as e:
                logger.error(f"Failed to retrieve lead status from CRM. {e}")
                return {
                    "statusCode": 202,
                    "body": dumps({"message": "Accepted. The request was received but failed to be processed by the CRM"})
                }
        else:
            logger.info(f"No lambda ARN detected for partner {partner_name}. Using status from DB.")
            status = lead_db.status

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
