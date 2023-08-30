"""Routes for server health checks."""
from api.cloudwatch import get_logger
from flask import Blueprint, jsonify, make_response

_logger = get_logger()

health_check_api = Blueprint("health_check_api", __name__)


@health_check_api.route("/health_check", methods=["GET"])
def get_health_check():
    """Health check endpoint."""
    try:
        return make_response(jsonify(success=True), 200)
    except Exception:
        _logger.exception("Error making request")
        return make_response(
            "Internal Server Error. Please contact Impel support.", 500
        )
