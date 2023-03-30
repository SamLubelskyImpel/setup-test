from api.s3_manager import upload_dms_data
from api.secrets_manager import check_api_key
from datetime import datetime, timezone
from flask import Blueprint, make_response, jsonify, current_app, request

fi_closed_deal_api = Blueprint("fi_closed_deal_api", __name__)


@fi_closed_deal_api.route("/v1", methods=["POST"])
def post_fi_closed_deal():
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    now_iso = now.isoformat()
    try:
        current_app.logger.info(
            f"{now_iso} Request: {request} Headers: {request.headers}"
        )

        client_id = request.headers["client_id"]
        x_api_key = request.headers["x_api_key"]

        if not check_api_key(client_id, x_api_key):
            return make_response("This request is unauthorized.", 401)

        filename = request.headers["filename"]
        data = request.data

        upload_dms_data(client_id, "fi_closed_deal", filename, data)

        response = {"file_name": filename, "received_date_utc": now_iso}
        current_app.logger.info(f"{now_iso} Response: {response}")
        return make_response(jsonify(response), 200)
    except Exception:
        current_app.logger.exception(f"{now_iso} Error making request")
        return make_response(
            "Internal Server Error. Please contact Impel support.", 500
        )
