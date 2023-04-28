"""Shared Flask functions."""
from flask import make_response
from api.cloudwatch import get_logger
_logger = get_logger()


def log_and_return_response(response: str, status_code: str):
    """Log response before returning."""
    _logger.info(f"Status: {status_code} Response: {response.get_data()}")
    return make_response(response, status_code)
