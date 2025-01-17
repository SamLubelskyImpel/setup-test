"""Retrieve available timeslots for a given dealer and service."""

import logging
from os import environ
from json import dumps, loads
from uuid import uuid4
from datetime import datetime
from utils import (invoke_vendor_lambda, IntegrationError, send_alert_notification,
                   get_dealer_info, get_vendor_op_code, ValidationError)

from appt_orm.session_config import DBSession

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "test")


def call_integration(payload, request_id, timeslots_arn) -> dict:
    """Invoke vendor integration lambda and process response."""
    response = invoke_vendor_lambda(payload, timeslots_arn)

    logger.info(f"Response from integration: {response}")
    if response["statusCode"] == 500:
        logger.error(f"Integration encountered error: {response}")
        body = loads(response["body"])
        return {
            "statusCode": 500,
            "body": dumps({
                "error": {
                    "code": body["error"]["code"],
                    "message": body["error"]["message"]
                },
                "request_id": request_id,
            })
        }
    elif response["statusCode"] != 200:
        raise IntegrationError(f"Vendor integration responded with status code {response['statusCode']}")

    # Parse response
    body = loads(response["body"])
    timeslots = body["available_timeslots"]
    logger.info(f"Timeslots: {timeslots}")
    return {
        "statusCode": 200,
        "body": dumps({
            "timeslots": timeslots,
            "request_id": request_id,
        })
    }


def lambda_handler(event, context):
    """Retrieve available timeslots for a given dealer and service."""
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    try:
        request_product = event["requestContext"]["authorizer"]["request_product"]
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]
        start_time = params["start_time"]
        end_time = params["end_time"]
        op_code = params["op_code"]

        vin = params.get("vin")
        year = params.get("year")
        make = params.get("make")
        model = params.get("model")
        if not vin and not (year and make and model):
            logger.error("VIN or Year, Make, Model must be provided")
            raise ValidationError("VIN or Year, Make, Model must be provided")

        start_time_dt = datetime.fromisoformat(start_time)
        end_time_dt = datetime.fromisoformat(end_time)
        if start_time_dt >= end_time_dt:
            logger.error("Start time must be before end time")
            return {
                "statusCode": 400,
                "body": dumps({
                    "error":  "Start time must be before end time",
                    "request_id": request_id,
                })
            }
        elif (end_time_dt.date() - start_time_dt.date()).days > 6:
            logger.error("Query date ranges must not exceed 6 days.")
            return {
                "statusCode": 400,
                "body": dumps({
                    "error": "Query date ranges must not exceed 6 days.",
                    "request_id": request_id,
                })
            }

        with DBSession() as session:
            # Get dealer info
            dealer_partner = get_dealer_info(
                session,
                dealer_integration_partner_id,
                request_product
            )
            if not dealer_partner:
                logger.error(f"No active dealer found with id {dealer_integration_partner_id} assigned to product {request_product}")
                return {
                    "statusCode": 404,
                    "body": dumps({
                        "error": f"No active dealer found with id {dealer_integration_partner_id} assigned to product {request_product}",
                        "request_id": request_id,
                    })
                }

            logger.info(f"Dealer integration partner: {dealer_partner}")
            dealer_timezone = dealer_partner.timezone
            integration_dealer_id = dealer_partner.integration_dealer_id
            partner_metadata = dealer_partner.metadata_

            # Get vendor op code
            op_code_result = get_vendor_op_code(session, dealer_integration_partner_id, op_code, dealer_partner.product_id)
            vendor_op_code = op_code_result.op_code if op_code_result else None
            if not vendor_op_code:
                logger.error(f"No integration op code mapping found for product op code: {op_code}")
                return {
                    "statusCode": 404,
                    "body": dumps({
                        "error": f"No integration op code mapping found for product op code: {op_code}",
                        "request_id": request_id,
                    })
                }
            logger.info(f"Product op code {op_code} mapped to vendor op code {vendor_op_code}")

        timeslots_arn = partner_metadata.get("timeslots_arn", "")
        if not timeslots_arn:
            raise Exception(f"Timeslots ARN not found in metadata for dealer integration partner {dealer_integration_partner_id}")

        payload = {
            "request_id": request_id,
            "integration_dealer_id": integration_dealer_id,
            "dealer_timezone": dealer_timezone,
            "op_code": vendor_op_code,
            "start_time": start_time,
            "end_time": end_time,
            "vin": params.get("vin"),
            "year": params.get("year"),
            "make": params.get("make"),
            "model": params.get("model")
        }
        logger.info(f"Payload to integration: {payload}")

        return call_integration(payload, request_id, timeslots_arn)

    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        return {
            "statusCode": 400,
            "body": dumps({
                "error": str(e),
                "request_id": request_id,
            })
        }
    except IntegrationError as e:
        logger.error(f"Integration error: {e}")
        send_alert_notification(request_id, "RetrieveTimeslots", e)
        return {
            "statusCode": 500,
            "body": dumps({
                "error": {
                    "code": "I002",
                    "message": "Unexpected response from vendor integration. Please contact Impel support."
                },
                "request_id": request_id,
            })
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        send_alert_notification(request_id, "RetrieveTimeslots", e)
        return {
            "statusCode": 500,
            "body": dumps({
                "error": {
                    "code": "I001",
                    "message": "Internal server error. Please contact Impel support."
                },
                "request_id": request_id,
            })
        }
