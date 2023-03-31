"""Routes for server health checks."""
from flask import Blueprint, make_response, current_app, jsonify

health_check_api = Blueprint("health_check_api", __name__)


@health_check_api.route("/health_check", methods=["GET"])
def get_health_check():
    """Health check endpoint."""
    try:
        return make_response(jsonify(success=True), 201)
    except Exception:
        current_app.logger.exception("Error making request")
        return make_response(
            "Internal Server Error. Please contact Impel support.", 500
        )
