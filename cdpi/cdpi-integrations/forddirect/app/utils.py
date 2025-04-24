from datetime import datetime, timezone
import requests
from os import environ
from json import loads, dumps
import boto3
import logging

from cdpi_orm.models.audit_dsr import AuditDsr

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT")
SNS_TOPIC_ARN = environ.get("ALERT_CLIENT_ENGINEERING_TOPIC")
EVENTS_LAMBDA_ARN = environ.get("EVENTS_LAMBDA_ARN")
IS_PROD = True if ENVIRONMENT == "prod" else False

secret_client = boto3.client("secretsmanager")
lambda_client = boto3.client("lambda")


def create_audit_dsr(integration_partner_id, consumer_id, event_type, dsr_request_id, complete_date=None, complete_flag=False):
    '''Method to create audit_dsr object'''
    datetime_now = datetime.now(timezone.utc)

    audit_dsr = AuditDsr(
        consumer_id=consumer_id,
        integration_partner_id=integration_partner_id,
        dsr_request_type=event_type,
        request_date=datetime_now,
        complete_flag=complete_flag,
        complete_date=complete_date,
        dsr_request_id=dsr_request_id
    )

    logger.info("Created audit_dsr successfully.")

    return audit_dsr


def call_events_api(
    event_type,
    integration_partner_id,
    consumer_id,
    source_consumer_id,
    dealer_id,
    salesai_dealer_id,
    serviceai_dealer_id,
    product_name
):
    '''Method to call events publishing api lambda function'''

    logger.info("call_events_api started")

    response = lambda_client.invoke(
        FunctionName=EVENTS_LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=dumps({"event_type": event_type, "integration_partner_id": integration_partner_id, "consumer_id": consumer_id,
                       "source_consumer_id": source_consumer_id, "dealer_id": dealer_id, "salesai_dealer_id": salesai_dealer_id,
                       "serviceai_dealer_id": serviceai_dealer_id, "product_name": product_name}),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode("utf-8"))
    logger.info(f"Payload: {response_json}")

    if response_json["statusCode"] != 204:
        logger.error(
            f"Error creating event on event publishing api: {response_json}"
        )
        raise

    logger.info("Event created on event publishing api")
    return loads(response_json["body"])


def send_alert_notification(request_id: str, endpoint: str, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=f"CDPI FORD DIRECT: {endpoint} Failure Alert",
        MessageStructure="json",
    )


def send_missing_files_notification(subject: str, details: dict) -> None:
    """Send missing inbound files or missing consumer profile summary notification to CE team."""
    data = {
        "message": f"{details}",
    }
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=subject,
        MessageStructure="json",
    )


def log_dev(log_msg):
    if not IS_PROD:
        logger.info(log_msg)
