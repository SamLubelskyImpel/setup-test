from os import environ
import boto3
from flask import current_app
from datetime import datetime, timezone
from botocore.exceptions import ClientError

ENV = environ.get("ENV", "stage")
REGION_NAME = environ.get("REGION_NAME", "us-east-1")

DMS_UPLOAD_BUCKET = f"{ENV}-dms-uploads-{REGION_NAME}"


def upload_dms_data(client_id, file_type, filename, data):
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    key = f"{client_id}/{file_type}/{now.year}/{now.month}/{now.day}/{filename}"

    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Body=data, Bucket=DMS_UPLOAD_BUCKET, Key=key)
    except ClientError:
        current_app.logger.exception(
            f"Error uploading {filename} to {key} for client {client_id}"
        )
        raise
