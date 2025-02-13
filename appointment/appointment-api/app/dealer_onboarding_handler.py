"""Dealers config handler."""

import logging
from os import environ
from json import dumps, loads
from typing import Any, Optional
from utils import is_valid_timezone
from database_manager import DatabaseManager, DealerOnboardingInfo, InvalidFilterException

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


class DealerOnboarding:
    def __init__(self, event: dict, db_manager: Optional[DatabaseManager] = None):
        self.method = event.get("httpMethod")
        self.body = self.get_body(event.get("body") or {})
        self.query_params = event.get("queryStringParameters") or {}
        self.db_manager = db_manager or DatabaseManager()

    def get_body(self, event_body) -> dict:
        """Parses the body from the event."""
        try:
            if isinstance(event_body, str):
                return loads(event_body)
            return event_body
        except Exception as e:
            logger.error(f"Unexpected error while getting a body: {str(e)}")
            raise ValueError(f"Error parsing body: {str(e)}")
        
    def handle_request(self) -> dict:
        handler_map = {
            "GET": self._handle_get,
            "POST": self._handle_post
        }
        handler = handler_map.get(self.method)
        if handler:
            try:
                return handler()
            except Exception as e:
                return {"statusCode": 404, "body": dumps({"error": str(e)})}

        return {"statusCode": 405, "body": dumps({"error": "Method Not Allowed"})}

    def _handle_get(self) -> dict:
        logger.info("Processing GET request")

        if not 'product_name' in self.query_params:
            logger.error("Invalid GET request: product_name query_param is required")
            return {
                "statusCode": 400,
                "body": dumps({"error": "Invalid GET request: product_name query_param is required."}),
            }
        
        self.db_manager.create_filters(self.query_params)
        dealer_records = self.db_manager.get_dealer_info()
        return {"statusCode": 200, "body": dumps(dealer_records)}

    def _handle_post(self) -> dict:
        logger.info("Processing POST request")
        
        try:
            return self.db_manager.post_dealer_onboarding_info(DealerOnboardingInfo(**self.body))
        except TypeError as e:
            logger.error(f"Invalid POST data: {str(e)}")
            return {
                "statusCode": 400,
                "body": dumps({"error": "Invalid POST data provided."}),
            }
    

def lambda_handler(event: Any, context: Any) -> Any:
    """Dealer onboarding handler."""
    logger.info(f"Received event: {dumps(event)}")
    try:
        dealer_onboarding_handler = DealerOnboarding(event)
        return dealer_onboarding_handler.handle_request()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }
