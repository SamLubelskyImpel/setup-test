from pydantic import BaseModel, ValidationError
from json import loads, JSONDecodeError
from os import environ
import logging

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

class ValidationErrorResponse(Exception):
    def __init__(self, errors, full_errors):
        self.errors = errors
        self.full_errors = full_errors

def sanitize_errors(errors):
    """
    Convert the detailed Pydantic errors into a simplified version for the user.
    For example, this function returns only the field name and a user-friendly message.
    """
    sanitized = []
    for error in errors:
        field = ".".join(map(str, error.get("loc", [])))
        message = error.get("msg", "Invalid input")
        sanitized.append({"field": field, "message": message})
    return sanitized

def validate_request_body(event: dict, model: BaseModel):
    """
    Validates the request body using a Pydantic model.

    Args:
        event (dict): The Lambda event object.
        model (BaseModel): The Pydantic model class to validate against.

    Returns:
        BaseModel: The validated model instance.

    Raises:
        ValidationErrorResponse: If validation fails.
    """
    try:
        body = loads(event.get("body", "{}"))
        return model(**body)
    except JSONDecodeError as json_err:
        logger.error("JSON decoding error: %s", json_err, exc_info=True)
        raise ValidationErrorResponse([
            {"field": "body", "message": "Invalid JSON format"}
        ])
    except ValidationError as e:
        logger.error("Validation error: %s", e, exc_info=True)
        sanitized = sanitize_errors(e.errors())
        raise ValidationErrorResponse(sanitized, e)
