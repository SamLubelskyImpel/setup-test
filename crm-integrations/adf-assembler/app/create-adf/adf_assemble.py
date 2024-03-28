import logging
from os import environ
from typing import Any
from boto3 import client
from json import dumps, loads
from datetime import datetime
from adf_creation_class import AdfCreation

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
ADF_SENDER_EMAIL_ADDRESS = environ.get("ADF_SENDER_EMAIL_ADDRESS", "")

s3_client = client("s3")
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(environ.get("LOGLEVEL", "INFO").upper()))

def lambda_handler(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        body = loads(event["body"]) if event.get("body") else event

        adf_creation = AdfCreation()
        formatted_adf, partner_id = adf_creation.create_adf_data(body.get("lead_id"), body.get("activity_time", ""))
        logger.info(f"[adf_assembler] adf file: \n{formatted_adf}")

        current_time = datetime.now().strftime("%Y_%m_%dT%H-%M-%SZ")
        logger.info(f"Partner ID: {partner_id}")

        s3_key = f"chatai/{partner_id}_{body.get('lead_id')}_{current_time}.json"

        logger.info(f"take object from: configurations/{ENVIRONMENT}_{partner_id}.json \n Bucket: {BUCKET}")
        s3_object = loads(
            s3_client.get_object(
                Bucket=BUCKET, Key=f"configurations/{ENVIRONMENT}_{partner_id}.json"
            )["Body"].read().decode("utf-8")
        )

        adf_integration_config = s3_object.get("adf_integration_config", {})
        integration_type = adf_integration_config.get("adf_integration_type")
        add_summary_to_appointment_comment = adf_integration_config.get("add_summary_to_appointment_comment", True) # default to True

        if integration_type == "EMAIL":
            recipients = adf_integration_config.get("recipients")


            logger.info(f"recipients: {recipients} \n integration_type: {integration_type}")

            s3_body = dumps(
                {
                    "recipients": recipients,
                    "subject": "Lead ADF From Impel",
                    "body": formatted_adf,
                    "from_address": ADF_SENDER_EMAIL_ADDRESS,
                    "reply_to": [],
                }
            )
            s3_client.put_object(
                Body=s3_body,
                Bucket=f"email-service-store-{ENVIRONMENT}",
                Key=s3_key,
            )
            return {
                "statusCode": 200,
                "body": dumps({"message": "Adf file was successfully created."})
            }
        else:
            logger.info(f"Unsupported integration type: {integration_type}")

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
