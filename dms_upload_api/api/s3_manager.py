"""S3 manager functionality."""
from os import environ
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from api.cloudwatch import get_logger
_logger = get_logger()


def upload_dms_data(client_id: str, file_type: str, filename: str, data: str):
    """Upload file to s3 dms uploads bucket."""
    ENV = environ.get("ENV", "test")
    REGION_NAME = environ.get("REGION_NAME", "us-east-1")
    DMS_UPLOAD_BUCKET = f"univ-integrations-{REGION_NAME}-{ENV}"
    
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    key = f"{client_id}/{file_type}/{now.year}/{now.month}/{now.day}/{filename}"

    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Body=data, Bucket=DMS_UPLOAD_BUCKET, Key=key)
    except ClientError:
        _logger.exception(
            f"Error uploading {filename} to {key} for client {client_id}"
        )
        raise
