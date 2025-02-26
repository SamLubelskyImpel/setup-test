from datetime import datetime
import pytz
import logging
from os import environ
from typing import Union
import boto3
from json import dumps

from crm_orm.models.dealer_integration_partner import DealerIntegrationPartner
from crm_orm.models.integration_partner import IntegrationPartner

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_restricted_query(query, integration_partner):
    if integration_partner:
        query = query.join(DealerIntegrationPartner.integration_partner).filter(IntegrationPartner.impel_integration_partner_name == integration_partner)
    return query


def format_date(date: datetime, include_tz_offset: bool):
    if include_tz_offset:
        formatted = date.strftime('%Y-%m-%dT%H:%M:%S%z')
        return formatted[:-2] + ':' + formatted[-2:]
    return date.strftime('%Y-%m-%dT%H:%M:%S')


def apply_dealer_timezone(input_ts: Union[str, datetime], time_zone: str, dealer_partner_id: str, include_tz_offset = False) -> str:
    """Convert UTC timestamp to dealer's local time."""
    utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%SZ') if isinstance(input_ts, str) else input_ts.replace(tzinfo=None)
    utc_datetime = pytz.utc.localize(utc_datetime)

    if not time_zone:
        logger.warning("Dealer timezone not found for dealer_partner: {}".format(dealer_partner_id))
        return format_date(utc_datetime, include_tz_offset)

    # Get the dealer timezone object, convert UTC datetime to dealer timezone
    dealer_tz = pytz.timezone(time_zone)
    dealer_datetime = utc_datetime.astimezone(dealer_tz)

    return format_date(dealer_datetime, include_tz_offset)

def send_general_alert_notification(subject, message) -> None:
    """Send alert notification to CE team."""
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({'default': dumps({"message": message})}),
        Subject=subject,
        MessageStructure='json'
    )