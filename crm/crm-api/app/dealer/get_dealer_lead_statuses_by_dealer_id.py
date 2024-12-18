"""Retrieve lead statuses by dealer_id."""

import boto3
import logging
from os import environ
from typing import Any
import botocore.exceptions
from json import dumps, loads


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
    try:
        s3_object = loads(
            s3_client.get_object(Bucket=BUCKET, Key=s3_key)["Body"]
            .read()
            .decode("utf-8")
        )
        lambda_arn = s3_object.get("get_dealer_lead_statuses_arn")
        return lambda_arn

    except botocore.exceptions.ClientError as e:
        logger.error(f"Error retrieving configuration file for {partner_name}")
        raise Exception(e)

    except Exception as e:
        logger.error(
            f"Failed to retrieve lambda ARN from S3 config. Partner: {partner_name.upper()}, {e}"
        )
        raise Exception(e)


def invoke_lambda_get_dealer_lead_statuses(dealer_id: str, lambda_arn: str):
    """Get dealer lead statuses from CRM."""
    logger.info(f"Invoke lambda to get lead statuses from CRM using crm_dealer_id:{dealer_id}")
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps({"crm_dealer_id": dealer_id}),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode("utf-8"))
    if response_json["statusCode"] != 200:
        logger.error(
            f"Error retrieving lead statuses {response_json['statusCode']}: {response_json}"
        )
        raise
    return loads(response_json["body"])

def parse_status_data(api_response):
    """
    Transforms the vendor API response to be formatted.
    """
    statuses_dict = {"generic_statuses": [], "custom_statuses": []}

    response_field = api_response.get('Response', {})
    status_list = response_field.get('SetupList', [])

    for status in status_list:
        status_name = status.get('Name', '')
        if 'SystemStatus' in status:
            statuses_dict["generic_statuses"].append(status_name)
        elif 'CustomStatus' in status:
            statuses_dict["custom_statuses"].append(status_name)

    return statuses_dict

def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve lead statuses by dealer id."""
    logger.info(f"Event: {event}")

    try:
        product_dealer_id = event["pathParameters"]["dealer_id"]

        with DBSession() as session:
            integration_partner_name = (
                session.query(
                    DealerIntegrationPartner.crm_dealer_id,
                    IntegrationPartner.impel_integration_partner_name,
                )
                .join(
                    Dealer,
                    Dealer.id == DealerIntegrationPartner.dealer_id,
                )
                .join(
                    IntegrationPartner,
                    IntegrationPartner.id
                    == DealerIntegrationPartner.integration_partner_id,
                )
                .filter(Dealer.product_dealer_id == product_dealer_id)
                .first()
            )

        lambda_arn = get_lambda_arn(integration_partner_name.impel_integration_partner_name)

        if not lambda_arn:
            return {
                "statusCode": 404,
                "body": dumps(
                    {
                        "error": f"This CRM don't support retrieving lead statuses list. dealer_id: {product_dealer_id}"
                    }
                ),
            }

        dealer_statuses = invoke_lambda_get_dealer_lead_statuses(
            dealer_id=integration_partner_name.crm_dealer_id, lambda_arn=lambda_arn
        )

        if not dealer_statuses:
            logger.error(
                f"No lead statuses found for dealer_id: {product_dealer_id}"
            )
            return {
                "statusCode": 404,
                "body": dumps(
                    {
                        "error": f"No lead statuses found for dealer_id: {product_dealer_id}"
                    }
                ),
            }

        logger.info(f"Found dealer lead statuses.")

        parsed_result = parse_status_data(dealer_statuses)
        logger.info("Successfully parsed response from API to: %s", parsed_result)

        return {
            "statusCode": 200,
            "body": dumps(parsed_result)
        }

    except Exception as e:
        logger.error(f"Error retrieving dealer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
