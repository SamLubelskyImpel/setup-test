import logging
import json
from os import environ
from typing import Any, Dict

from cdpi_orm.session_config import DBSession
from dealer_repository import DealerRepository, ValidationErrorResponse

# Configure logger with a format that includes timestamp and level for CloudWatch
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=environ.get("LOGLEVEL", "INFO").upper()
)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler entry point for dealer configurations."""
    method = event.get("httpMethod")
    body_str = event.get("body", "{}")
    logger.info("Received request with method: %s and body: %s", method, body_str)

    try:
        # Validate JSON here so that subsequent operations don't have to wrap JSON decoding.
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError:
            logger.error("Invalid JSON in request body")
            return _response(400, {"message": "Invalid JSON in request body"})

        with DBSession() as session:
            repo = DealerRepository(session)
            
            if method == "GET":
                return _handle_get(event, repo)
            elif method == "POST":
                return _handle_post(body, repo)
            elif method == "PUT":
                return _handle_put(body, repo)
            else:
                logger.warning("Method %s not allowed", method)
                return _response(405, {"message": f"Method {method} not allowed"})
    except ValidationErrorResponse as e:
        logger.warning("Validation error: %s", e.full_errors)
        return _response(400, {"message": "Validation failed", "errors": e.errors})
    except Exception as e:
        logger.error("Error processing request: %s", str(e), exc_info=True)
        return _response(500, {"message": "Internal server error", "error": str(e)})


def _handle_get(event: Dict[str, Any], repo: DealerRepository) -> Dict[str, Any]:
    """Handle GET requests: retrieve dealers with filters and pagination."""
    filter_params = event.get("queryStringParameters") or {}
    try:
        page = int(filter_params.pop("page", 1))
        limit = int(filter_params.pop("limit", 100))
    except ValueError:
        logger.error("Invalid pagination parameters: %s", filter_params)
        return _response(400, {"message": "Invalid pagination parameters: page and limit must be integers."})

    logger.info("Processing GET with filters %s, page=%s, limit=%s", filter_params, page, limit)
    dealers, has_next_page = repo.get_dealers(filter_params, page, limit)
    logger.info("Retrieved %s dealers, has_next_page=%s", len(dealers), has_next_page)
    return _response(200, {"page": page, "limit": limit, "dealers": dealers, "has_next_page": has_next_page})


def _handle_post(body: Dict[str, Any], repo: DealerRepository) -> Dict[str, Any]:
    """Handle POST requests: create a new dealer."""
    logger.debug("Processing POST with body: %s", body)
    new_dealer = repo.create_dealer(body)
    logger.info("Created dealer with id: %s", new_dealer.id)
    return _response(201, {"message": "Dealer created successfully", "dealer_id": new_dealer.id})


def _handle_put(body: Dict[str, Any], repo: DealerRepository) -> Dict[str, Any]:
    """Handle PUT requests: update an existing dealer."""
    logger.debug("Processing PUT with body: %s", body)
    dealer_id = body.get("dealer_id")
    if dealer_id is None:
        logger.error("Missing dealer_id in update request")
        return _response(400, {"message": "dealer_id is required for update."})
    
    updated_dealer = repo.update_dealer(dealer_id, body)
    logger.info("Updated dealer with id: %s", dealer_id)
    return _response(200, updated_dealer.as_dict(), default=str)


def _response(status_code: int, body: Dict[str, Any], **json_dumps_kwargs) -> Dict[str, Any]:
    """Helper function to build a Lambda proxy response."""
    return {
        "statusCode": status_code,
        "body": json.dumps(body, **json_dumps_kwargs)
    }
