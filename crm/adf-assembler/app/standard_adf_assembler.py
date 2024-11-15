import logging
from os import environ
from typing import Any, Dict
from boto3 import client
from json import dumps, loads
from datetime import datetime
from shared.adf_creation_class import AdfCreation
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
ADF_SENDER_EMAIL_ADDRESS = environ.get("ADF_SENDER_EMAIL_ADDRESS", "")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = client("s3")
processor = BatchProcessor(event_type=EventType.SQS)


def standard_adf_assembler(body: Dict[str, Any]) -> Dict[str, Any]:
    required_keys = ["partner_name", "lead_id"]
    for key in required_keys:
        if key not in body:
            logger.error(f"Missing required field: {key}")
            raise ValueError(f"Missing required field: {key}")

    partner_name = body["partner_name"]
    lead_id = body["lead_id"]

    current_time = datetime.now().strftime("%Y_%m_%dT%H-%M-%SZ")
    filename = f"{partner_name}_{lead_id}_{current_time}"
    s3_key = f"chatai/{filename}.json"

    logger.info(
        f"Fetching configuration for partner: {partner_name} from bucket: {BUCKET}"
    )
    try:
        config_object = s3_client.get_object(
            Bucket=BUCKET,
            Key=f"configurations/{ENVIRONMENT}_{partner_name}.json"
        )["Body"].read().decode("utf-8")
        s3_object = loads(config_object)
    except Exception as e:
        logger.exception(f"Error fetching configuration for partner {partner_name}: {e}")
        raise

    adf_integration_config = s3_object.get("adf_integration_config", {})
    integration_type = adf_integration_config.get("adf_integration_type", "EMAIL")
    add_summary_to_appointment_comment = adf_integration_config.get(
        "add_summary_to_appointment_comment", True
    )

    adf_creation = AdfCreation()
    formatted_adf = adf_creation.create_adf_data(
        lead_id=lead_id,
        appointment_time=body.get("activity_time", ""),
        add_summary_to_appointment_comment=add_summary_to_appointment_comment,
    )

    logger.info(f"ADF file created: {formatted_adf}")

    if integration_type == "EMAIL":
        recipients = body.get("recipients", [])
        s3_body = dumps({
            "recipients": recipients,
            "subject": "Lead ADF From Impel",
            "body": formatted_adf,
            "from_address": ADF_SENDER_EMAIL_ADDRESS,
            "reply_to": [],
        })
        s3_client.put_object(
            Body=s3_body,
            Bucket=f"email-service-store-{ENVIRONMENT}",
            Key=s3_key,
        )
        return {"statusCode": 200, "body": dumps({"message": "ADF file successfully created."})}

    elif integration_type == "SFTP":
        sftp_config = body.get("sftp_config")
        if not sftp_config:
            raise ValueError("SFTP configuration is missing.")
        import sftp
        sftp.put_adf(sftp_config, formatted_adf, f"{filename}.xml")
        return {"statusCode": 200, "body": dumps({"message": "ADF file uploaded to SFTP."})}

    else:
        logger.error(f"Unsupported integration type: {integration_type}")
        raise ValueError(f"Unsupported integration type: {integration_type}")

def record_handler(record: SQSRecord) -> None:
    """Process each SQS record."""
    logger.info(f"Processing record with message ID: {record.message_id}")
    standard_adf_assembler(record.json_body)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point to process events."""
    logger.info("Starting batch event processing.")
    try:
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Critical error processing batch: {e}")
        raise
