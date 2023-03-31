"""DMS Upload API Flask app entry point."""
from os import environ
import logging
from flask import Flask
from api.route.repair_order import repair_order_api
from api.route.fi_closed_deal import fi_closed_deal_api
from api.route.health_check import health_check_api


def create_app():
    """Create DMS Upload API flask app."""
    environ["ENV"] = "stage"
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    app.register_blueprint(repair_order_api, url_prefix="/repair-order")
    app.register_blueprint(fi_closed_deal_api, url_prefix="/fi-closed-deal")
    app.register_blueprint(health_check_api)

    app.config.from_pyfile("config.py")

    return app


app = create_app()
