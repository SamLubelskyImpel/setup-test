from typing import Any
from os import environ
from json import loads, dumps
from logging import getLogger

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def parse_event(event: Any) -> Any:
    """Parse the event to extract the body or return the raw event if no body is found."""
    try:
        if 'body' in event:
            return loads(event['body'])
        return event
    except ValueError as e:
        logger.error(f"Failed to parse event body: {e}")
        raise ValueError("Invalid JSON format in event body.")

def validate_data(data: dict, data_class: type) -> Any:
    """Validate data and initialize the data class."""
    try:
        return data_class(**data)
    except TypeError as e:
        logger.error(f"Invalid data for {data_class.__name__}: {e}")
        raise ValueError(f"Invalid data for {data_class.__name__}: {str(e)}")
    

def handle_exception(e, context):
    logger.exception(f"Error in {context}: {e}")
    return {
        "statusCode": 500,
        "body": dumps(
            {
                "error": "An error occurred while processing the request.",
                "details": str(e),
            }
        ),
    }


def lambda_response(status_code, body):
    return {"statusCode": status_code, "body": dumps(body)}

