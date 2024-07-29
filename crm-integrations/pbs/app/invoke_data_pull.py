import boto3
import logging
from os import environ
from json import dumps, loads
from typing import Any
import requests
from requests.auth import HTTPBasicAuth
from uuid import uuid4
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
secret_client = boto3.client("secretsmanager")

def lambda_handler(event: Any, context: Any) -> Any:
    logger.info(f"Event: {event}")
    return