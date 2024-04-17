import logging
from os import environ
from json import dumps, loads
from uuid import uuid4
from utils import invoke_vendor_lambda, IntegrationError

from appt_orm.session_config import DBSession
from appt_orm.models.dealer_integration_partner import DealerIntegrationPartner
from appt_orm.models.dealer import Dealer
from appt_orm.models.integration_partner import IntegrationPartner
from appt_orm.models.op_code import OpCode
from appt_orm.models.op_code import OpCodeProduct
from appt_orm.models.op_code import OpCodeAppointment

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def call_integration(payload, request_id, timeslots_arn) -> dict:
    # Invoke vendor lambda
    response = invoke_vendor_lambda(payload, timeslots_arn)
    if response["statusCode"] == 500:
        logger.error(f"Integration encountered error: {response}")
        body = loads(response["body"])
        return {
            "statusCode": "500",
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
    timeslots = []
    for slot in body["available_timeslots"]:
        timeslots.append({
            "timeslot": slot["timeslot"],
            "duration": slot["duration"],
        })

    return {
        "statusCode": "200",
        "body": dumps({
            "timeslots": timeslots,
            "request_id": request_id,
        })
    }


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    try:
        params = event["queryStringParameters"]
        dealer_integration_partner_id = params["dealer_integration_partner_id"]
        start_time = params["start_time"]
        end_time = params["end_time"]
        op_code = params["op_code"]

        with DBSession() as session:
            # Get dealer info
            dealer_partner = session.query(
                DealerIntegrationPartner.id, DealerIntegrationPartner.product_id,
                DealerIntegrationPartner.integration_dealer_id,
                Dealer.timezone, IntegrationPartner.metadata_
            ).join(
                Dealer, Dealer.id == DealerIntegrationPartner.dealer_id
            ).join(
                IntegrationPartner, IntegrationPartner.id == DealerIntegrationPartner.integration_partner_id
            ).filter_by(
                id=dealer_integration_partner_id,
                is_active=True
            ).first()

            if not dealer_partner:
                return {
                    "statusCode": "404",
                    "body": dumps({
                        "error": f"No active dealer found with id {dealer_integration_partner_id}",
                        "request_id": request_id,
                    })
                }

            logger.info(f"Dealer integration partner: {dealer_partner}")
            dealer_timezone = dealer_partner.timezone
            integration_dealer_id = dealer_partner.integration_dealer_id
            partner_metadata = dealer_partner.metadata_

            # Get vendor op code
            vendor_op_code = session.query(
                OpCode.op_code
            ).join(
                OpCodeAppointment, OpCodeAppointment.op_code_id == OpCode.id
            ).join(
                OpCodeProduct, OpCodeProduct.id == OpCode.op_code_product_id
            ).filter(
                OpCode.dealer_integration_partner_id == dealer_integration_partner_id,
                OpCodeProduct.product_id == dealer_partner.product_id,
                OpCodeProduct.op_code == op_code
            ).first()

            if not vendor_op_code:
                return {
                    "statusCode": "404",
                    "body": dumps({
                        "error": f"No integration op code mapping found for product op code: {op_code}",
                        "request_id": request_id,
                    })
                }
            logger.info(f"Product op code {op_code} mapped to vendor op code {vendor_op_code}")

        timeslots_arn = loads(partner_metadata).get("timeslots_arn", "")
        if not timeslots_arn:
            raise Exception(f"Timeslots ARN not found in metadata for partner {integration_dealer_id}")

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

        return call_integration(payload, request_id, timeslots_arn)

    except IntegrationError as e:
        logger.error(f"Integration error: {e}")
        return {
            "statusCode": "500",
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
        return {
            "statusCode": "500",
            "body": dumps({
                "error": {
                    "code": "I001",
                    "message": "Internal server error. Please contact Impel support."
                },
                "request_id": request_id,
            })
        }
