"""Utility functions for the appointment service."""

import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
from dateutil import parser as date_parser
import pytz

from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.dealer import Dealer
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.op_code import OpCode
from appt_orm.models.service_type import ServiceType
from appt_orm.models.product import Product

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")


class IntegrationError(Exception):
    pass


class ValidationError(Exception):
    pass


def invoke_vendor_lambda(payload: dict, lambda_arn: str) -> Any:
    """Invoke vendor integration lambda."""
    response = boto3.client("lambda").invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps(payload),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode("utf-8"))
    return response_json



def is_valid_timezone(dealer_timezone: str) -> bool:
    if dealer_timezone not in pytz.all_timezones:
        return False
    return True

def format_timestamp(local_timestamp: Any, timezone: Any) -> Any:
    """Convert local time to UTC."""
    parsed_ts = date_parser.parse(local_timestamp)

    # Check if the timestamp is already in UTC (ends with 'Z')
    if (
        parsed_ts.tzinfo is not None
        and parsed_ts.tzinfo.utcoffset(parsed_ts) is not None
    ):
        # Timestamp is either UTC or has an offset; return in ISO format
        return parsed_ts.isoformat()

    dealer_tz = pytz.timezone(timezone)
    # Localize the timestamp to the dealer's timezone
    localized_ts = dealer_tz.localize(parsed_ts)
    return localized_ts.isoformat()


def convert_utc_to_timezone(input_ts, timezone, dealer_partner_id) -> str:
    """Convert UTC timestamp to dealer's local time."""
    if not timezone:
        logger.warning(
            "Dealer timezone not found for dealer_partner: {}".format(dealer_partner_id)
        )
        return input_ts.strftime("%Y-%m-%dT%H:%M:%S")

    # Get the dealer timezone object, convert UTC datetime to dealer timezone
    dealer_tz = pytz.timezone(timezone)
    dealer_datetime = input_ts.astimezone(dealer_tz)

    return dealer_datetime.strftime("%Y-%m-%dT%H:%M:%S")


def send_alert_notification(request_id: str, endpoint: str, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=f"Appointment Service: {endpoint} Failure Alert",
        MessageStructure="json",
    )


def get_dealer_info(
    session, dealer_integration_partner_id: str, request_product=None
) -> dict:
    """Get dealer info from the shared layer."""
    query = (
        session.query(
            DealerIntegrationPartner.id,
            DealerIntegrationPartner.product_id,
            DealerIntegrationPartner.integration_dealer_id,
            Dealer.timezone,
            IntegrationPartner.metadata_,
            Product.product_name,
        )
        .join(Dealer, Dealer.id == DealerIntegrationPartner.dealer_id)
        .join(
            IntegrationPartner,
            IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id,
        )
        .join(Product, Product.id == DealerIntegrationPartner.product_id)
        .filter(
            DealerIntegrationPartner.id == dealer_integration_partner_id,
            DealerIntegrationPartner.is_active == True,
        )
    )

    if request_product:
        query = query.filter(Product.product_name == request_product)

    dealer_partner = query.first()
    return dealer_partner


def get_vendor_op_code(session, dealer_integration_partner_id: str, service_type: str) -> str:
    """Get vendor op code from the shared layer."""
    op_code_result = (
        session.query(OpCode.op_code, OpCode.id)
        .join(ServiceType, ServiceType.id == OpCode.service_type_id)
        .filter(
            OpCode.dealer_integration_partner_id == dealer_integration_partner_id,
            ServiceType.service_type == service_type,
        )
        .first()
    )

    return op_code_result


def validate_request_body(body: Any, required_params) -> None:
    """Validate request body."""
    for param in required_params:
        if isinstance(param, dict):
            for key, value in param.items():
                if key not in body:
                    raise ValidationError(
                        f"Required parameter '{key}' not found in request body"
                    )
                validate_request_body(body[key], value)
        elif param not in body:
            raise ValidationError(
                f"Required parameter '{param}' not found in request body"
            )
