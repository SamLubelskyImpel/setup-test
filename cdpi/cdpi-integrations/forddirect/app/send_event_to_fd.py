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
from utils import log_dev

logger = logging.getLogger(__name__)
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

secret_client = boto3.client("secretsmanager")

ENVIRONMENT = environ.get("ENVIRONMENT", "test")


def get_secret(secret_name: str) -> Dict[str, Any]:
    """Retrieve a secret from AWS Secrets Manager."""
    try:
        logger.info(f"[Secrets] Fetching secret | SecretName: {secret_name}")
        response = secret_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response["SecretString"])
        logger.info(f"[Secrets] Successfully retrieved secret | SecretName: {secret_name}")
        return secret_data
    except boto3.exceptions.Boto3Error as e:
        logger.exception(f"[Secrets] Failed to retrieve secret | SecretName: {secret_name} | Error: {e}")
        raise


def send_request_to_fd(endpoint: str, headers: Dict[str, str], body: Dict[str, Any]) -> Dict[str, Any]:
    """Send a POST request to the Ford Direct DSR API."""
    try:
        logger.info(f"[HTTP] Sending request | Endpoint: {endpoint} | Headers: {headers} | Payload: {body}")
        response = post(endpoint, headers=headers, json=body)
        response.raise_for_status()

        logger.info(f"[HTTP] Request successful | Endpoint: {endpoint} | StatusCode: {response.status_code}")
        log_dev(f"Response: {response.json()}")
        return response.json()
    except RequestException as e:
        logger.exception(f"[HTTP] Error sending request | Endpoint: {endpoint} | Error: {e}")
        raise Exception("Failed to send request.")


def record_handler(record: SQSRecord):
    """Process a single SQS record and send a request to Ford Direct DSR."""
    try:
        logger.info(f"[SQS] Processing record | MessageBody: {record.body}")
        data = json.loads(record.body)

        consumer_id = data.get("consumer_id")
        dealer_id = data.get("dealer_id")
        event_type = data.get("event_type")
        dsr_request_id = data.get("dsr_request_id")
        completed_flag = data.get("completed_flag", False)
        is_enterprise = data.get("is_enterprise", "0")

        if not consumer_id or not dealer_id or not event_type:
            logger.warning(f"[Validation] Missing required fields | Data: {data}")
            raise ValueError("Missing required fields: consumer_id, dealer_id, or event_type")

        secret_name = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/CDPI/FD-DSR"
        secret = get_secret(secret_name)

        fd_url = secret["url_v1"]
        header, value = secret["api_key"].split(':')
        fd_headers = {header: value, "Content-Type": "application/json"}

        fd_request_body = {
            "record_timestamp": datetime.now(timezone.utc).isoformat(),
            "is_enterprise": is_enterprise if is_enterprise else "0",  # '0' means DSR request is on dealer level
            "ext_consumer_id": consumer_id,
            "dealer_identifier": dealer_id,
            "response": "1" if completed_flag else "0",
        }

        if event_type == "cdp.dsr.optout":
            fd_request_body["dsr_optout_request_id"] = dsr_request_id
            fd_url += "/optout/response"
        else:
            fd_request_body["dsr_delete_request_id"] = dsr_request_id
            fd_url += "/delete/response"

        logger.info(f"[SQS] Sending request to FD | RequestID: {dsr_request_id} | Endpoint: {fd_url}")
        send_request_to_fd(fd_url, fd_headers, fd_request_body)

    except json.JSONDecodeError as e:
        logger.error(f"[JSON] Error decoding JSON | Error: {e}")
        raise Exception("Invalid JSON format")
    except Exception as e:
        logger.exception(f"[Handler] Unexpected error processing record | Error: {e}")
        raise Exception("Internal server error")


def lambda_handler(event: Any, context: Any):
    """Lambda function entry point for processing SQS messages."""
    logger.info(f"[Lambda] Event received | Event: {json.dumps(event)}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        return process_partial_response(
            event=event, record_handler=record_handler, processor=processor, context=context
        )
    except Exception as e:
        logger.exception(f"[Lambda] Critical error processing records | Error: {e}")
        raise
