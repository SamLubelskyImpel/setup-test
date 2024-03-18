import logging
from os import environ
from typing import Any
from boto3 import client
from json import dumps, loads
from datetime import datetime
from api_wrapper import ApiWrapper

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENV", "test")

s3_client = client("s3")
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(environ.get("LOGLEVEL", "INFO").upper()))

def lambda_handler(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        body = loads(event["body"]) if event.get("body") else event

        api_wrapper = ApiWrapper()
        formatted_adf, partner_id = api_wrapper.get_lead(body.get("lead_id"))
        logger.info(f"[adf_assembler] adf file: \n{formatted_adf}")

        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Partner ID: {partner_id}")

        s3_key = f"email-service-store-test/chatai/{partner_id}_{body.get('lead_id')}_{current_time}.json"

        logger.info(f"take object from: configurations/{ENVIRONMENT}_{partner_id}.json \n Bucket: {BUCKET}")

        s3_object = loads(
            s3_client.get_object(
                Bucket=BUCKET, Key=f"configurations/{ENVIRONMENT}_{partner_id}.json"
            )["Body"].read().decode("utf-8")
        )
        recipients = s3_object.get("recipients")
        integration_type = s3_object.get("adf_integration_type")

        logger.info(f"recipients: {recipients} \n integration_type: {integration_type}")

        if integration_type == "EMAIL":
            s3_body = dumps(
                {
                    "recipients": recipients,
                    "subject": "Lead ADF From Impel",
                    "body": formatted_adf,
                    "from_address": "crm.adf@impel.ai",
                    "reply_to": [],
                }
            )
            s3_client.put_object(
                Body=s3_body,
                Bucket=f"crm-integrations-test",
                Key=s3_key,
            )
            return {
                "statusCode": 200,
                "body": dumps({"message": "Adf file was successfully created."})
            }
    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
