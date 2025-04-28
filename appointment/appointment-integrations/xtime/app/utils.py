"""Utility functions for the XTime integration."""

from typing import Any
from os import environ
from json import loads, dumps
from logging import getLogger
from datetime import datetime
import boto3

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

def parse_event(event: Any) -> Any:
    """Parse the event to extract the body or return the raw event if no body is found."""
    try:
        if "body" in event:
            return loads(event["body"])
        return event
    except ValueError as e:
        logger.error(f"Failed to parse event body: {e}")
        raise ValueError("Invalid JSON format in event body.")


def validate_data(data: dict, data_class: type) -> Any:
    """Validate data and initialize the data class."""
    try:
        instance = data_class(**data)
        attributes = instance.__dataclass_fields__.keys()
        if "vin" in attributes:
            if not instance.vin and not (
                instance.year and instance.make and instance.model
            ):
                raise ValueError(
                    "Either VIN or Year, Make, and Model combination must be provided"
                )
        return instance
    except TypeError as e:
        logger.error(f"Invalid data for {data_class.__name__}: {e}")
        raise ValueError(f"Invalid data for {data_class.__name__}: {str(e)}")

def send_alert_notification(request_id: str, endpoint: str, error: str) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {error}",
    }
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=f"Appointment Integration Xtime: {endpoint} Failure Alert",
        MessageStructure="json",
    )

def handle_response(request_id, endpoint, status_code, body, exception=None) -> dict:
    """Generates lambda response for runtime success or error/exception."""
    if exception:
        logger.exception(f"Error in {endpoint}: {exception}")
        send_alert_notification(request_id=request_id, endpoint=endpoint, error=exception)

    elif body.get("error"):
        logger.error(f"Error in {endpoint}: {body['error']['message']}")
        send_alert_notification(request_id=request_id, endpoint=endpoint, error=body["error"]["message"])
    
    else:
        logger.info(f"Success in {endpoint}: {body}")
    
    return {
        "statusCode": status_code,
        "body": dumps(body)
    }


def formatted_time(time_string: str) -> str:
    dt = datetime.fromisoformat(time_string.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def format_and_filter_timeslots(timeslots: list, start_time: str, end_time: str) -> list:
    """Filter time slots by start and end time."""
    if not timeslots:
        return []

    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)

    filtered_timeslots = []
    for time_slot in timeslots:
        slot = formatted_time(time_slot["appointmentDateTimeLocal"])
        duration = time_slot["durationMinutes"]

        if start_dt <= datetime.fromisoformat(slot) <= end_dt:
            filtered_timeslots.append(
                {
                    "timeslot": slot,
                    "duration": duration,
                }
            )
    return filtered_timeslots
