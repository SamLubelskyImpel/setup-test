"""Create lead in the shared CRM layer."""
import logging
from os import environ
from typing import Any
from boto3 import client
from json import dumps, loads
from api_wrapper import ApiWrapper
from adf_samples import STANDARD_ADF_FORMAT, APPOINTMENT_ADF
from datetime import datetime

BUCKET = environ.get("INTEGRATIONS_BUCKET")

s3_client = client("s3")
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        body = loads(event["body"])
        logger.info(f"Body: {body}")

        api_wrapper = ApiWrapper()
        lead_data = api_wrapper.get_lead(body.get("lead_id"))
        vehicles_of_interest = lead_data.get("vehicles_of_interest", [{}])[0]
        logger.log(f"[adf_assembler] data from lead_id: \n{dumps(lead_data)}")

        appt_time = body.get("activity_time")
        if appt_time:
            appointment = APPOINTMENT_ADF.format(
                activity_time=datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
            )

        full_name = " ".join(
            filter(
                None,
                (lead_data.get(x) for x in ("first_name", "middle_name", "last_name")),
            )
        )

        formatted_adf = STANDARD_ADF_FORMAT.format(
            request_date=datetime.now(),
            year=vehicles_of_interest.get("year"),
            make=vehicles_of_interest.get("make"),
            model=vehicles_of_interest.get("model"),
            full_name=full_name,
            phone=lead_data.get("phone"),
            email=lead_data.get("email"),
            appointment=appointment,
            vendor_full_name=lead_data.get("dealer_name"),
        )
        logger.log(f"[adf_assembler] adf file: \n{formatted_adf}")

        s3_key = f"adf/chat_ai/{body.get("lead_id")}_{datetime.now()}.json"

        s3_client.put_object(
            Body=dumps({"adf_file":formatted_adf}),
            Bucket=BUCKET,
            Key=s3_key,
        )
    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
