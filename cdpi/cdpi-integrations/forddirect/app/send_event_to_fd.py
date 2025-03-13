import boto3
import logging
import json
from os import environ
from typing import Any, Dict
from datetime import datetime, timezone
from requests import post, RequestException
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from uuid import uuid4

logger = logging.getLogger(__name__)
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

secret_client = boto3.client("secretsmanager")

ENVIRONMENT = environ.get("ENVIRONMENT", "test")

def get_secret(secret_name: str) -> Dict[str, Any]:
    """Retrieve a secret from AWS Secrets Manager."""
    try:
        response = secret_client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_name}: {str(e)}")
        raise


def send_request_to_fd(endpoint: str, headers: Dict[str, str], body: Dict[str, Any]) -> Dict[str, Any]:
    """Send a POST request to the Ford Direct DSR API."""
    try:
        response = post(endpoint, headers=headers, json=body)
        response.raise_for_status()
        logger.info(f"Successfully sent request to {endpoint}")
        return {"statusCode": response.status_code, "body": response.json()}
    except RequestException as e:
        logger.error(f"Error sending request to {endpoint}: {str(e)}")
        return {"statusCode": 500, "body": {"error": "Failed to send request"}}


def record_handler(record: SQSRecord) -> Dict[str, Any]:
    """Process a single SQS record and send a request to Ford Direct DSR."""
    try:
        data = json.loads(record.body)

        source_consumer_id = data["source_consumer_id"]
        source_dealer_id = data["source_dealer_id"]
        event_type = data["event_type"]
        request_id = str(uuid4())

        # ✅ Retrieve secret values
        secret_name = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/CDPI/FD-DSR"
        secret = get_secret(secret_name)

        fd_url = secret["url_v1"]
        fd_headers = {"x-api-key": secret["api_key"], "Content-Type": "application/json"}

        # ✅ Build request body
        fd_request_body = {
            "record_timestamp": datetime.now(timezone.utc).isoformat(),
            "is_enterprise": "0",  # '0' means DSR request is on dealer level
            "ext_consumer_id": source_consumer_id,
            "dealer_identifier": source_dealer_id,
            "response": "1",
        }

        if event_type == "cdp.dsr.optout":
            fd_request_body["dsr_optout_request_id"] = request_id
            fd_url += "/optout/response"
        else:
            fd_request_body["dsr_delete_request_id"] = request_id
            fd_url += "/delete/response"

        # ✅ Send request
        response = send_request_to_fd(fd_url, fd_headers, fd_request_body)
        logger.info(f"Response from Ford Direct DSR: {response}")

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {str(e)}")
        return {"statusCode": 400, "body": {"error": "Invalid JSON format"}}
    except Exception as e:
        logger.exception(f"Unexpected error processing record: {str(e)}")
        return {"statusCode": 500, "body": {"error": "Internal server error"}}


def lambda_handler(event: Any, context: Any):
    """Lambda function entry point for processing SQS messages."""
    logger.info(f"Received Event: {json.dumps(event)}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        return process_partial_response(event=event, record_handler=record_handler, processor=processor, context=context)
    except Exception as e:
        logger.exception(f"Error processing records: {str(e)}")
        raise
