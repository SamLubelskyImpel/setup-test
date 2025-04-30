import logging
from os import environ
from typing import Any
from boto3 import client
from json import dumps, loads
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from shared.adf_creation_class import AdfCreation

BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
ADF_SENDER_EMAIL_ADDRESS = environ.get("ADF_SENDER_EMAIL_ADDRESS", "")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = client("s3")
processor = BatchProcessor(event_type=EventType.SQS)


def record_handler(record: SQSRecord) -> None:
    """Process each SQS record."""
    logger.info(f"Processing record with message ID: {record.message_id}")
    body = record.json_body
    logger.info(f"Body: {body}")

    required_keys = ["partner_name", "lead_id", "event_type", "product_dealer_id"]
    for key in required_keys:
        if key not in body:
            logger.error(f"Missing required field: {key}")
            raise ValueError(f"Missing required field: {key}")

    partner_name = body["partner_name"]
    lead_id = body["lead_id"]
    product_dealer_id = body["product_dealer_id"]
    current_time = datetime.now().strftime("%Y_%m_%dT%H-%M-%SZ")
    filename = f"{partner_name}_{lead_id}_{current_time}"
    s3_key = f"chatai/{filename}.json"

    logger.info(
        f"Fetching configuration for partner: {partner_name} from bucket: {BUCKET}"
    )
    try:
        config_object = s3_client.get_object(
            Bucket=BUCKET,
            Key=f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_{partner_name}.json"
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
    dealer = adf_creation.call_crm_api(f"https://{CRM_API_DOMAIN}/dealers/{product_dealer_id}")
    dip_metadata = dealer.get("metadata", {})

    adf_recipients = dip_metadata.get("adf_email_recipients", [])
    sftp_config = dip_metadata.get("adf_sftp_config", {})
    remove_xml_tag = dip_metadata.get("remove_xml_tag", False)

    formatted_adf = adf_creation.create_adf_data(
        lead_id=lead_id,
        dealer=dealer,
        appointment_time=body.get("activity_time", ""),
        add_summary_to_appointment_comment=add_summary_to_appointment_comment,
        remove_xml_tag=remove_xml_tag,
    )

    logger.info(f"ADF file created: {formatted_adf}")

    if integration_type == "EMAIL":
        s3_body = dumps({
            "recipients": adf_recipients,
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
        return

    elif integration_type == "SFTP":
        if not sftp_config:
            raise ValueError("SFTP configuration is missing.")
        import sftp
        sftp.put_adf(sftp_config, formatted_adf, f"{filename}.xml")
        return

    else:
        logger.error(f"Unsupported integration type: {integration_type}")
        raise ValueError(f"Unsupported integration type: {integration_type}")


def lambda_handler(event: Any, context: Any):
    """Lambda function handler."""
    logger.info("Lambda invocation started.")
    try:
        return process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
    except Exception as e:
        logger.error(f"Critical error in batch processing: {e}")
        raise
