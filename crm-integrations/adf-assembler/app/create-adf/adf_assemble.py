import logging
from os import environ
from typing import Any
from boto3 import client
from json import dumps, loads
from datetime import datetime
from api_wrapper import ApiWrapper
from adf_samples import STANDARD_ADF_FORMAT, APPOINTMENT_ADF

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENV", "test")

s3_client = client("s3")
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(environ.get("LOGLEVEL", "INFO").upper()))


def assemble_adf(body, lead_data):
    full_name = " ".join(
        filter(
            None,
            (lead_data.get(x) for x in ("first_name", "middle_name", "last_name")),
        )
    )

    appointment = ""
    appt_time = body.get("activity_time")
    if appt_time:
        appointment = APPOINTMENT_ADF.format(
            activity_time=datetime.strptime(appt_time, "%Y-%m-%dT%H:%M:%SZ")
        )

    vehicles_of_interest = lead_data.get("vehicles_of_interest", [{}])[0]

    return STANDARD_ADF_FORMAT.format(
        request_date=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        year=vehicles_of_interest.get("year"),
        make=vehicles_of_interest.get("make"),
        model=vehicles_of_interest.get("model"),
        full_name=full_name,
        phone=lead_data.get("phone"),
        email=lead_data.get("email"),
        appointment=appointment,
        vendor_full_name=lead_data.get("dealer_name"),
    )


def lambda_handler(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        body = loads(event["body"]) if event.get("body") else event

        api_wrapper = ApiWrapper()
        lead_data = api_wrapper.get_lead(body.get("lead_id"))
        logger.info(f"[adf_assembler] data from lead_id: \n{lead_data}")

        formatted_adf = assemble_adf(body, lead_data)
        logger.info(f"[adf_assembler] adf file: \n{formatted_adf}")

        partner_id = lead_data.get("integration_partner_name")
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Partner ID: {partner_id}")

        s3_key = f"chatai/{partner_id}_{body.get('lead_id')}_{current_time}.json"

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
                Bucket=f"email-service-store-{ENVIRONMENT}",
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
