from flask import make_response, current_app


def log_and_return_response(response, status_code):
    current_app.logger.info(f"Status: {status_code} Response: {response.get_data()}")
    return make_response(response, status_code)
