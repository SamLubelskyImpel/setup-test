import logging
import json
from os import environ
from typing import Any, Dict

from cdpi_orm.session_config import DBSession
from dealer_repository import DealerRepository, ValidationErrorResponse, ErrorMessage

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler entry point for dealer configurations."""
    method = event.get("httpMethod")
    body_str = event.get("body") or "{}"
    logger.info(f"Received request with method: {method} and body: {body_str}")

    try:
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
                logger.warning(f"Method {method} not allowed")
                return _response(405, {"message": f"Method {method} not allowed"})
    except ErrorMessage as e:
        logger.error(f"Error message: {e.message}")
        return _response(e.status_code, {"message": e.message})
    except ValidationErrorResponse as e:
        logger.warning(f"Validation error: {e.full_errors}")
        return _response(400, {"message": "Validation failed", "errors": e.errors})
    except Exception as e:
        logger.error("Error processing request: %s", str(e), exc_info=True)
        return _response(500, {"message": "Internal server error", "error": str(e)})


def _handle_get(event: Dict[str, Any], repo: DealerRepository) -> Dict[str, Any]:
    """Handle GET requests: retrieve dealers with filters and pagination."""
    filter_params = event.get("queryStringParameters") or {}
    dealers_data = repo.get_dealers(filter_params)
    logger.info(f"Retrieved {len(dealers_data['response'])} dealers, has_next_page={dealers_data['has_next_page']}")
    return _response(200, dealers_data)


def _handle_post(body: Dict[str, Any], repo: DealerRepository) -> Dict[str, Any]:
    """Handle POST requests: create a new dealer."""
    logger.debug(f"Processing POST with body: {body}")
    new_dealer = repo.create_dealer(body)
    logger.info(f"Created dealer with id: {new_dealer.id}")
    return _response(201, {"message": "Dealer created successfully", "dealer_id": new_dealer.id})


def _handle_put(body: Dict[str, Any], repo: DealerRepository) -> Dict[str, Any]:
    """Handle PUT requests: update an existing dealer."""
    logger.debug(f"Processing PUT with body: {body}")
    dealer_id = body.get("dealer_id")
    if dealer_id is None:
        logger.error("Missing dealer_id in update request")
        return _response(400, {"message": "dealer_id is required for update."})
    
    updated_dealer, is_updated = repo.update_dealer(dealer_id, body)
    if not is_updated:
        return _response(200, {"message": f"No updates were applied for dealer id: {dealer_id}"})
    logger.info(f"Updated dealer with id: {dealer_id}")
    return _response(200, updated_dealer.as_dict(), default=str)


def _response(status_code: int, body: Dict[str, Any], **json_dumps_kwargs) -> Dict[str, Any]:
    """Helper function to build a Lambda proxy response."""
    return {
        "statusCode": status_code,
        "body": json.dumps(body, **json_dumps_kwargs)
    }
