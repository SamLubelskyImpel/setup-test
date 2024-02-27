"""Routes for uploading crm files."""
import uuid
import boto3
from datetime import datetime, timezone
from os import environ
from json import loads, dumps

from api.cloudwatch import get_logger
from api.flask_common import log_and_return_response
from api.s3_manager import upload_crm_data
from api.secrets_manager import check_basic_auth, decode_basic_auth
from api.sns_manager import send_email_notification
from flask import Blueprint, jsonify, request

_logger = get_logger()

crm_upload_api = Blueprint("crm_upload_api", __name__)
s3_client = boto3.client("s3")


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


def get_lambda_arn():
    """Get lambda ARN from S3."""
    ENVIRONMENT = environ.get("ENV", "test")
    CRM_UPLOAD_BUCKET = f"crm-integrations-{ENVIRONMENT}"
    s3_key = f"configurations/{ENVIRONMENT}_REYREY.json"
    _logger.info(f"Getting lambda ARN from S3. Bucket: {CRM_UPLOAD_BUCKET} Key: {s3_key}")
    try:
        s3_object = loads(
                s3_client.get_object(
                    Bucket=CRM_UPLOAD_BUCKET,
                    Key=s3_key
                )['Body'].read().decode('utf-8')
            )
        lambda_arn = s3_object.get("process_historical_data_arn")
    except Exception as e:
        _logger.error(f"Failed to retrieve lambda ARN from S3 config. Partner: REYREY, {e}")
        raise
    return lambda_arn


def process_historical_data(key: str):
    """Process historical data."""
    ENVIRONMENT = environ.get("ENV", "test")
    CRM_UPLOAD_BUCKET = f"crm-integrations-{ENVIRONMENT}"
    REGION_NAME = environ.get("REGION_NAME", "us-east-1")
    try:
        lambda_client = boto3.client("lambda", region_name=REGION_NAME)
        payload = {
            "bucket_name": CRM_UPLOAD_BUCKET,
            "file_name": key,
        }

        lambda_arn = get_lambda_arn()

        response = lambda_client.invoke(
            FunctionName=lambda_arn,
            InvocationType='Event',
            Payload=dumps(payload).encode('utf-8')
        )
    except Exception:
        _logger.exception(f"Error processing historical data for {key}")
        raise


@crm_upload_api.route("/v1", methods=["POST"])
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

        key = upload_crm_data(client_id, file_type, filename, data)

        if client_id == "reyrey_crm" or client_id == "test_client":
            process_historical_data(key)

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
