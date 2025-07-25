"""S3 manager functionality."""
from datetime import datetime, timezone
from os import environ

import boto3
from api.cloudwatch import get_logger
from botocore.exceptions import ClientError

_logger = get_logger()


def upload_dms_data(client_id: str, file_type: str, filename: str, data: str):
    """Upload file to s3 dms uploads bucket."""
    ENV = environ.get("ENV", "test")
    REGION_NAME = environ.get("REGION_NAME", "us-east-1")
    DMS_UPLOAD_BUCKET = f"integrations-{REGION_NAME}-{ENV}"

    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    key = f"{client_id}/{file_type}/{now.year}/{now.month}/{now.day}/{filename}"

    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Body=data, Bucket=DMS_UPLOAD_BUCKET, Key=key)
    except ClientError:
        _logger.exception(f"Error uploading {filename} to {key} for client {client_id}")
        raise


def upload_crm_data(client_id: str, file_type: str, filename: str, data: str):
    """Upload file to s3 crm uploads bucket."""
    ENV = environ.get("ENV", "test")
    CRM_UPLOAD_BUCKET = f"crm-integrations-{ENV}"

    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    key = f"historical_data/raw/{client_id}/{file_type}/{now.year}/{now.month}/{now.day}/{now.hour}/{filename}"

    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Body=data, Bucket=CRM_UPLOAD_BUCKET, Key=key)
        return key
    except ClientError:
        _logger.exception(f"Error uploading {filename} to {key} for client {client_id}")
        raise
