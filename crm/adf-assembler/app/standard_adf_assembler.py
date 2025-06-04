import logging
from os import environ
from typing import Any
from boto3 import client
import sftp
from json import dumps, loads
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from shared.adf_creation_class import AdfCreation
from shared.shared_class import BaseClass as CRMAPIWrapper


BUCKET = environ.get("INTEGRATIONS_BUCKET")
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
ADF_SENDER_EMAIL_ADDRESS = environ.get("ADF_SENDER_EMAIL_ADDRESS", "")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = client("s3")
processor = BatchProcessor(event_type=EventType.SQS)


def parse_datetime(time_str: str) -> str:
    """Parse datetime string and convert to standard format, regardless of input format."""
    try:
        parsed_time = datetime.fromisoformat(time_str)
        time_str = parsed_time.replace(tzinfo=None).isoformat()
        if time_str.endswith('T00:00:00'):
            time_str = time_str.split('T')[0]
        return time_str
    except ValueError:
        logger.exception(f"Error parsing datetime string: {time_str}")
        raise


def get_adf_settings(partner_name: str) -> tuple[str, bool]:
    """Get ADF settings for a partner."""
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
        s3_object = {}

    adf_config = s3_object.get("adf_integration_config", {})
    integration_type = adf_config.get("adf_integration_type", "EMAIL")
    add_summary_to_appointment_comment = adf_config.get(
        "add_summary_to_appointment_comment", True
    )
    return integration_type, add_summary_to_appointment_comment


def perform_adf_creation(partner_name: str, lead_id: str, dealer_db: dict, activity_time: str) -> None:
    """Perform ADF creation and forwarding."""
    # Fetching partner ADF configuration
    integration_type, add_summary_to_appointment_comment = get_adf_settings(partner_name)

    # ADF Creation
    adf_creation = AdfCreation()
    dip_metadata = dealer_db.get("metadata", {})
    adf_recipients = dip_metadata.get("adf_email_recipients", [])
    sftp_config = dip_metadata.get("adf_sftp_config", {})
    remove_xml_tag = dip_metadata.get("remove_xml_tag", False)

    formatted_adf = adf_creation.create_adf_data(
        lead_id=lead_id,
        dealer=dealer_db,
        appointment_time=activity_time,
        add_summary_to_appointment_comment=add_summary_to_appointment_comment,
        remove_xml_tag=remove_xml_tag,
    )

    logger.info(f"ADF file created: {formatted_adf}")

    # Forwarding ADF to email service or SFTP
    current_time = datetime.now().strftime("%Y_%m_%dT%H-%M-%SZ")
    filename = f"{partner_name}_{lead_id}_{current_time}"

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
            Key=f"chatai/{filename}.json",
        )
    elif integration_type == "SFTP":
        if not sftp_config:
            raise ValueError("SFTP configuration is missing.")

        sftp.put_adf(sftp_config, formatted_adf, f"{filename}.xml")
    else:
        logger.error(f"Unsupported integration type: {integration_type}")
        raise ValueError(f"Unsupported integration type: {integration_type}")


def record_handler(record: SQSRecord) -> None:
    """Process each SQS record."""
    logger.info(f"Processing record with message ID: {record.message_id}")
    body = record.json_body
    logger.info(f"Body: {body}")

    if not (body.get("id") and body.get("detail-type")):
        logger.info("SQS message received. Ignored.")
        return

    logger.info("EventBridge message received.")
    data = body.get("detail", {})

    # Data Extraction
    lead_id = data["lead_id"]
    activity_id = data.get("activity_id", "")
    idp_dealer_id = data["idp_dealer_id"]
    partner_name = data["partner_name"]

    crm_api_wrapper = CRMAPIWrapper()
    event_type = data["event_type"]

    # Handle Activity Created event
    activity_time = ""
    if event_type == "Activity Created":
        if not activity_id:
            raise ValueError("Activity ID is required for Activity Created event type")
        activity_db = crm_api_wrapper.get_activity(activity_id)
        if activity_db["activity_type"] == "appointment":
            logger.info("Activity is an appointment, processing...")
            activity_time = parse_datetime(activity_db["activity_due_ts"])
        else:
            logger.info("Activity is not an appointment, skipping...")
            return

    dealer_db = crm_api_wrapper.get_idp_dealer(idp_dealer_id)

    perform_adf_creation(partner_name, lead_id, dealer_db, activity_time)


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
