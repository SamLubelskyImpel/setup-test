import json
from pydantic import BaseModel, ValidationError

class ValidationErrorResponse(Exception):
    def __init__(self, errors):
        self.errors = errors

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
        # body = json.loads(event.get("body", "{}"))
        # return model(**body)
        return model(**event)
    except json.JSONDecodeError:
        raise ValidationErrorResponse([{"loc": ["body"], "msg": "Invalid JSON format", "type": "json"}])
    except ValidationError as e:
        raise ValidationErrorResponse(e.errors())
