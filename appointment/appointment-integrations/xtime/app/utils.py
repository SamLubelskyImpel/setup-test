"""Utility functions for the XTime integration."""

from typing import Any
from os import environ
from json import loads, dumps
from logging import getLogger
from datetime import datetime

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


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


def handle_exception(e, context):
    """Generates lambda response for runtime exceptions."""
    logger.exception(f"Error in {context}: {e}")
    return {
        "statusCode": 500,
        "body": dumps(
            {
                "error": {
                    "code": "V001",
                    "message": "Vendor integration an unexpected error. Please contact Impel support.",
                }
            }
        ),
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
