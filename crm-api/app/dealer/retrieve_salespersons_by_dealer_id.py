"""Retrieve salespersons by dealer_id."""

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
        lambda_arn = s3_object.get("get_dealer_salespersons_arn")
        return lambda_arn

    except botocore.exceptions.ClientError as e:
        logger.error(f"Error retrieving configuration file for {partner_name}")
        raise Exception(e)

    except Exception as e:
        logger.error(
            f"Failed to retrieve lambda ARN from S3 config. Partner: {partner_name.upper()}, {e}"
        )
        raise Exception(e)


def invoke_lambda_get_dealer_salespersons(dealer_id: str, lambda_arn: str):
    """Get dealer salespersons from CRM."""
    logger.info(f"Invoke lambda to get salespersons from CRM using crm_dealer_id:{dealer_id}")
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps({"crm_dealer_id": dealer_id}),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode("utf-8"))
    if response_json["statusCode"] != 200:
        logger.error(
            f"Error retrieving salespersons {response_json['statusCode']}: {response_json}"
        )
        raise
    return loads(response_json["body"])


def lambda_handler(event: Any, context: Any) -> Any:
    """Retrieve dealer by id."""
    logger.info(f"Event: {event}")

    try:
        salespersons_list = []
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
                        "error": f"This CRM don't support retrieving salespersons list. dealer_id: {product_dealer_id}"
                    }
                ),
            }

        dealer_salespersons = invoke_lambda_get_dealer_salespersons(
            dealer_id=integration_partner_name.crm_dealer_id, lambda_arn=lambda_arn
        )

        if not dealer_salespersons:
            logger.error(
                f"No salespersons found with dealer_id: {product_dealer_id}"
            )
            return {
                "statusCode": 404,
                "body": dumps(
                    {
                        "error": f"No dealer found with dealer_id: {product_dealer_id}"
                    }
                ),
            }

        logger.info(f"Found dealer salespersons: {len(dealer_salespersons)}")

        for salesperson in dealer_salespersons:
            salespersons_list.append(
                {
                    "Emails": salesperson.get('email', []),
                    "FirstName": salesperson.get('first_name'),
                    "FullName": f"{salesperson.get('first_name')} {salesperson.get('last_name')}",
                    "LastName": salesperson.get('last_name'),
                    "Phones": salesperson.get('phone', []),
                    "UserId": salesperson.get('crm_salesperson_id'),
                    "Role": salesperson.get('role')
                }
            )

        return {
            "statusCode": 200,
            "body": dumps(salespersons_list),
        }

    except Exception as e:
        logger.error(f"Error retrieving dealer: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
