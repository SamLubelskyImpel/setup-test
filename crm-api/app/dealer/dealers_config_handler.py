"""Dealers config handler."""

import logging
from os import environ
from json import dumps, loads
from typing import Any, Optional

from database_manager import DatabaseManager, DealerInfo, DealerStatus

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class DealersConfig:
    def __init__(self, event: dict, db_manager: Optional[DatabaseManager] = None):
        self.event = event
        self.method = event.get("httpMethod")
        self.body = event.get("body", {})
        self.query_params = event.get("queryStringParameters", {})
        self.db_manager = db_manager or DatabaseManager()

    def handle_request(self) -> dict:
        handler_map = {
            "GET": self._handle_get,
            "POST": self._handle_post,
            "PUT": self._handle_put,
        }
        handler = handler_map.get(self.method)
        if handler:
            return handler()
        return self._method_not_allowed_response()

    def _handle_get(self) -> dict:
        logger.info("Processing GET request")
        self.db_manager.create_filters(self.query_params)
        dealer_records = self.db_manager.get_dealers_config()
        return {"statusCode": 200, "body": dumps(dealer_records)}

    def _handle_post(self) -> dict:
        logger.info("Processing POST request")
        try:
            return self.db_manager.post_dealers_config(DealerInfo(**self.body))
        except TypeError as e:
            logger.error(f"Invalid POST data: {str(e)}")
            return {
                "statusCode": 400,
                "body": dumps({"error": "Invalid POST data provided."}),
            }

    def _handle_put(self) -> dict:
        logger.info("Processing PUT request")
        try:
            dealer_status = DealerStatus(**self.body)
            return self.db_manager.put_dealers_config(dealer_status)
        except TypeError as e:
            logger.error(f"Invalid PUT data: {str(e)}")
            return {
                "statusCode": 400,
                "body": dumps({"error": "Invalid PUT data provided."}),
            }

    def _method_not_allowed_response(self) -> dict:
        return {"statusCode": 405, "body": dumps({"error": "Method Not Allowed"})}


def lambda_handler(event: Any, context: Any) -> Any:
    """Dealer configs handler."""
    logger.info(f"Received event: {dumps(event)}")
    try:
        dealers_config_handler = DealersConfig(event)
        return dealers_config_handler.handle_request()
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {"statusCode": 400, "body": dumps({"error": "Invalid request data."})}
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
