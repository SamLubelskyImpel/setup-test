"""Shared Flask functions."""
from api.cloudwatch import get_logger
from flask import make_response

_logger = get_logger()


def log_and_return_response(response: str, status_code: str):
    """Log response before returning."""
    _logger.info(f"Status: {status_code} Response: {response.get_data()}")
    return make_response(response, status_code)
