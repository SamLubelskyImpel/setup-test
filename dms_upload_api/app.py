"""DMS Upload API Flask app entry point."""
from os import environ
import logging
from flask import Flask
from api.route.dms_upload import dms_upload_api
from api.route.health_check import health_check_api


def create_app():
    """Create DMS Upload API flask app."""
    environ["ENV"] = "stage"
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    app.register_blueprint(dms_upload_api, url_prefix="/dms-upload")
    app.register_blueprint(health_check_api)

    app.config.from_pyfile("config.py")

    return app


app = create_app()
