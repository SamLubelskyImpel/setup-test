import uuid
from api.s3_manager import upload_dms_data
from api.secrets_manager import check_api_key
from api.flask_common import log_and_return_response
from datetime import datetime, timezone
from flask import Blueprint, jsonify, current_app, request

fi_closed_deal_api = Blueprint("fi_closed_deal_api", __name__)


@fi_closed_deal_api.route("/v1", methods=["POST"])
def post_fi_closed_deal():
    request_id = str(uuid.uuid4())
    try:
        now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
        current_app.logger.info(
            f"ID: {request_id} Request: {request} Headers: {request.headers}"
        )

        client_id = request.headers.get("client_id", None)
        if not client_id:
            response = {
                "message": "Error missing header client_id",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)
        x_api_key = request.headers.get("x_api_key", None)
        if not x_api_key:
            response = {
                "message": "Error missing header x_api_key",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)
        filename = request.headers.get("filename", None)
        if not filename:
            response = {
                "message": "Error missing header filename",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)

        if not check_api_key(client_id, x_api_key):
            response = {
                "message": "This request is unauthorized",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 401)

        data = request.data

        upload_dms_data(client_id, "fi_closed_deal", filename, data)

        response = {
            "file_name": filename,
            "received_date_utc": now.isoformat(),
            "request_id": request_id,
        }
        return log_and_return_response(jsonify(response), 200)
    except Exception:
        response = {
            "message": "Internal Server Error. Please contact Impel support",
            "request_id": request_id,
        }
        return log_and_return_response(jsonify(response), 500)
