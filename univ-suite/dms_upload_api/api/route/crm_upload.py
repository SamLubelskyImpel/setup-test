"""Routes for uploading crm files."""
import uuid
from datetime import datetime, timezone

from api.cloudwatch import get_logger
from api.flask_common import log_and_return_response
from api.s3_manager import upload_crm_data
from api.secrets_manager import check_basic_auth, decode_basic_auth
from api.sns_manager import send_email_notification
from flask import Blueprint, jsonify, request

_logger = get_logger()

crm_upload_api = Blueprint("dms_upload_api", __name__)


def get_reyrey_file_type(filename: str):
    """Categorize ReyRey file types by filename.
    Filenames are _ deliminated with the 3rd parameter specifying type."""
    reyrey_file_type_mappings = {
        "SL": "sales_lead"
    }
    file_type = None
    filename_sections = filename.split("_")
    if len(filename_sections) >= 3:
        file_type = reyrey_file_type_mappings.get(filename_sections[2])
    return file_type


@crm_upload_api.route("/crm-upload-batch/v1", methods=["POST"])
def post_crm_upload():
    """Upload Repair Order file."""
    request_id = str(uuid.uuid4())
    try:
        now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
        _logger.info(f"ID: {request_id} Request: {request} Headers: {request.headers}")

        auth = request.headers.get("Authorization", None)
        if not auth:
            response = {
                "message": "Error missing header Authorization",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)
        filename = request.headers.get("Filename", None)
        if not filename:
            response = {
                "message": "Error missing header Filename",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)

        client_id, input_auth = decode_basic_auth(auth)
        if (
            not client_id
            or not input_auth
            or not check_basic_auth(client_id, input_auth)
        ):
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
        if client_id == "reyrey_crm" or client_id == "test_client":
            file_type = get_reyrey_file_type(filename)

        if not file_type:
            response = {
                "message": "Error invalid filetype",
                "request_id": request_id,
            }
            return log_and_return_response(jsonify(response), 400)

        upload_crm_data(client_id, file_type, filename, data)

        response = {
            "file_name": filename,
            "received_date_utc": now.isoformat(),
            "request_id": request_id,
        }
        return log_and_return_response(jsonify(response), 200)
    except Exception:
        _logger.exception("Error running crm_upload endpoint")
        send_email_notification(f"Error running crm_upload endpoint. Request ID: {request_id}")
        response = {
            "message": "Internal Server Error. Please contact Impel support",
            "request_id": request_id,
        }
        return log_and_return_response(jsonify(response), 500)
