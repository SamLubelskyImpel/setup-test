"""Shared Flask functions."""
from flask import make_response, current_app


def log_and_return_response(response: str, status_code: str):
    """Log response before returning."""
    current_app.logger.info(f"Status: {status_code} Response: {response.get_data()}")
    return make_response(response, status_code)
