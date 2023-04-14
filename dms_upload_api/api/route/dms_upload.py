"""Routes for uploading DMS files."""
import uuid
from api.s3_manager import upload_dms_data
from api.secrets_manager import check_api_key
from api.flask_common import log_and_return_response
from datetime import datetime, timezone
from flask import Blueprint, jsonify, current_app, request

dms_upload_api = Blueprint("dms_upload_api", __name__)


def get_reyrey_file_type(filename: str):
    """ Categorize ReyRey file types by filename.
    Filenames are _ deliminated with the 3rd parameter specifying type. """
    reyrey_file_type_mappings = {
        "RO": "repair_order",
        "DH": "fi_closed_deal"
    }
    file_type = None
    filename_sections = filename.split("_")
    if len(filename_sections) >= 3:
        file_type = reyrey_file_type_mappings.get(filename_sections[2])
    return file_type


@dms_upload_api.route("/v1", methods=["POST"])
def post_dms_upload():
    """Upload Repair Order file."""
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
        if not data:
            response = {
                "message": "Error missing request data",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)

        file_type = None
        if client_id == "reyrey" or client_id == "test_client":
            file_type = get_reyrey_file_type(filename)

        if not file_type:
            response = {
                "message": "Error invalid filetype",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)

        upload_dms_data(client_id, file_type, filename, data)

        response = {
            "file_name": filename,
            "received_date_utc": now.isoformat(),
            "request_id": request_id,
        }
        return log_and_return_response(jsonify(response), 200)
    except Exception:
        current_app.logger.exception("Error running dms_upload endpoint")
        response = {
            "message": "Internal Server Error. Please contact Impel support",
            "request_id": request_id,
        }
        return log_and_return_response(jsonify(response), 500)
