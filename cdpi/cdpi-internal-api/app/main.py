import logging
from os import environ
from json import dumps, loads
from typing import Any

from cdpi_orm.session_config import DBSession
from dealer_repository import DealerRepository

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Dealer configs handler."""
    path = event.get("path")
    method = event.get("httpMethod")
    body = event.get("body", "{}")

    logger.info("Path: %s,\n Method: %s,\n Body: %s", path, method, body)

    try:
        with DBSession() as session:
            repo = DealerRepository(session)
            
            if method == "GET":
                # Retrieve query parameters if provided
                filter_params = event.get("queryStringParameters") or {}

                try:
                    page = int(filter_params.pop("page", 1))
                    limit = int(filter_params.pop("limit", 100))
                except ValueError:
                    return {
                        "statusCode": 400,
                        "body": dumps({"message": "Invalid pagination parameters: page and limit must be integers."})
                    }
                
                dealers, has_next_page = repo.get_dealers(filter_params)
                logger.info(f"Retrieved {len(dealers)} dealers and has_next_page={has_next_page}")
                response = {
                    "page": page,
                    "limit": limit,
                    "dealers": [dealer.as_dict() for dealer in dealers],
                    "has_next_page": has_next_page
                }

                return {
                    "statusCode": 200,
                    "body": dumps(response)
                }

            elif method == "POST":
                # Create a new dealer and its integration partner record
                dealer_data = loads(body)
                new_dealer = repo.create_dealer(dealer_data)
                return {
                    "statusCode": 201,
                    "body": dumps({
                        "message": "Dealer created successfully",
                        "dealer_id": new_dealer.id
                    })
                }            

            elif method == "PUT":
                # Update an existing dealer
                update_data = loads(body)
                dealer_id = update_data.get("dealer_id")
                if dealer_id is None:
                    return {
                        "statusCode": 400,
                        "body": dumps({"message": "dealer_id is required for update."})
                    }
                updated_dealer = repo.update_dealer(dealer_id, update_data)
                return {
                    "statusCode": 200,
                    "body": dumps(updated_dealer.as_dict())
                }
            
            else:
                return {
                    "statusCode": 405,
                    "body": dumps({"message": f"Method {method} not allowed on {path}"})
                }

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            "statusCode": 500,
            "body": dumps({"message": "Internal server error", "error": str(e)})
        }
    