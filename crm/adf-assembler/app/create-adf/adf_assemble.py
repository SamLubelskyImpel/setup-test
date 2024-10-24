import logging
from os import environ
from typing import Any
from boto3 import client
from json import dumps, loads
from datetime import datetime
from adf_creation_class import AdfCreation
from oem_adf_creation import OemAdfCreation
import sftp

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

        partner_name = body.get("partner_name", "")
        logger.info(f"Partner Name: {partner_name}")

        current_time = datetime.now().strftime("%Y_%m_%dT%H-%M-%SZ")
        filename = f"{partner_name}_{body.get('lead_id')}_{current_time}"
        s3_key = f"chatai/{filename}.json"

        logger.info(
            f"take object from: configurations/{ENVIRONMENT}_{partner_name}.json \n Bucket: {BUCKET}"
        )
        s3_object = loads(
            s3_client.get_object(
                Bucket=BUCKET, Key=f"configurations/{ENVIRONMENT}_{partner_name}.json"
            )["Body"]
            .read()
            .decode("utf-8")
        )

        adf_integration_config = s3_object.get("adf_integration_config", {})
        integration_type = adf_integration_config.get("adf_integration_type", "EMAIL")
        add_summary_to_appointment_comment = adf_integration_config.get(
            "add_summary_to_appointment_comment", True
        )  # default to True
        oem_recipient = body.get("oem_recipient", "").lower()


        if oem_recipient:
            oem_class = OemAdfCreation(oem_recipient)
            is_vehicle_of_interest = oem_class.create_adf_data(body.get('lead_id'))
            if is_vehicle_of_interest:
                return {
                    "statusCode": 200,
                    "body": dumps({"message": "Adf file was successfully sended to JDPA API."}),
                }

        adf_creation = AdfCreation()
        formatted_adf = adf_creation.create_adf_data(
            lead_id=body.get("lead_id"),
            appointment_time=body.get("activity_time", ""),
            add_summary_to_appointment_comment=add_summary_to_appointment_comment,
        )
        logger.info(f"[adf_assembler] adf file: \n{formatted_adf}")

        if integration_type == "EMAIL":
            recipients = body.get("recipients", [])

            logger.info(
                f"recipients: {recipients} \n integration_type: {integration_type}"
            )

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
                "body": dumps({"message": "Adf file was successfully created."}),
            }
        elif integration_type == "SFTP":
            sftp_config = body.get("sftp_config")

            if not sftp_config:
                raise Exception("SFTP configuration is missing")

            sftp.put_adf(sftp_config, formatted_adf, f"{filename}.xml")

            return {
                "statusCode": 200,
                "body": dumps(
                    {"message": "Adf file was successfully uploaded to SFTP."}
                ),
            }
        else:
            logger.info(f"Unsupported integration type: {integration_type}")

    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        raise
