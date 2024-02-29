"""Create lead in the shared CRM layer."""
import logging
from os import environ
from json import dumps, loads
from typing import Any, List

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

def lambda_handler(event: Any, context: Any) -> Any:
    try:
        logger.info(f"Event: {event}")
        body = loads(event["body"])
        logger.info(f"Body: {body}") # Delete after testing
        
    except Exception as e:
        logger.exception(f"Error creating lead: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."})
        }
