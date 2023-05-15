"""DMS Upload API Flask app entry point."""
import logging
from json import loads
from os import environ

from api.route.dms_upload import dms_upload_api
from api.route.health_check import health_check_api
from api.secrets_manager import get_secret
from flask import Flask

REGION_NAME = environ.get("REGION_NAME", "us-east-1")


def create_app():
    """Create DMS Upload API flask app."""
    secret = get_secret("universal-integrations/deployment_info", REGION_NAME)
    environ["ENV"] = loads(secret["SecretString"])["ENVIRONMENT"]
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    app.register_blueprint(dms_upload_api, url_prefix="/dms-upload")
    app.register_blueprint(health_check_api)

    app.config.from_pyfile("config.py")

    return app


app = create_app()
