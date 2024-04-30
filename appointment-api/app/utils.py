import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
# from datetime import datetime
from dateutil import parser as date_parser
import pytz

from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.dealer import Dealer
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.op_code import OpCode
from appt_orm.models.op_code_product import OpCodeProduct
from appt_orm.models.op_code_appointment import OpCodeAppointment

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")


class IntegrationError(Exception):
    pass


def invoke_vendor_lambda(payload: dict, lambda_arn: str) -> Any:
    """Invoke vendor lambda."""
    response = boto3.client("lambda").invoke(
        FunctionName=lambda_arn,
        InvocationType="RequestResponse",
        Payload=dumps(payload),
    )
    logger.info(f"Response from lambda: {response}")
    response_json = loads(response["Payload"].read().decode('utf-8'))
    return response_json


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
    # utc_datetime = datetime.strptime(input_ts, '%Y-%m-%d %H:%M:%S%z')
    # utc_datetime = pytz.utc.localize(input_ts)

    if not timezone:
        logger.warning("Dealer timezone not found for dealer_partner: {}".format(dealer_partner_id))
        return input_ts.strftime('%Y-%m-%dT%H:%M:%S')

    # Get the dealer timezone object, convert UTC datetime to dealer timezone
    dealer_tz = pytz.timezone(timezone)
    dealer_datetime = input_ts.astimezone(dealer_tz)

    return dealer_datetime.strftime('%Y-%m-%dT%H:%M:%S')


def send_alert_notification(request_id: str, endpoint: str, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({'default': dumps(data)}),
        Subject=f'Appointment Service: {endpoint} Failure Alert',
        MessageStructure='json'
    )


def get_dealer_info(session, dealer_integration_partner_id: str) -> dict:
    """Get dealer info from the shared layer."""
    dealer_partner = session.query(
        DealerIntegrationPartner.id, DealerIntegrationPartner.product_id,
        DealerIntegrationPartner.integration_dealer_id,
        Dealer.timezone, IntegrationPartner.metadata_
    ).join(
        Dealer, Dealer.id == DealerIntegrationPartner.dealer_id
    ).join(
        IntegrationPartner, IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id
    ).filter(
        DealerIntegrationPartner.id == dealer_integration_partner_id,
        DealerIntegrationPartner.is_active == True
    ).first()

    return dealer_partner


def get_vendor_op_code(session, dealer_integration_partner_id: str, op_code: str, product_id: str) -> str:
    """Get vendor op code from the shared layer."""
    op_code_result = session.query(
        OpCode.op_code, OpCodeAppointment.id
    ).join(
        OpCodeAppointment, OpCodeAppointment.op_code_id == OpCode.id
    ).join(
        OpCodeProduct, OpCodeProduct.id == OpCodeAppointment.op_code_product_id
    ).filter(
        OpCode.dealer_integration_partner_id == dealer_integration_partner_id,
        OpCodeProduct.product_id == product_id,
        OpCodeProduct.op_code == op_code
    ).first()

    return op_code_result
