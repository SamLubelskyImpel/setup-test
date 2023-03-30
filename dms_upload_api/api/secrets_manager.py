from os import environ
import boto3
from flask import current_app
from json import loads
from botocore.exceptions import ClientError

ENV = environ.get("ENV", "stage")
REGION_NAME = environ.get("REGION_NAME", "us-east-1")

SECRET_ID = f"{ENV}/DMSUploadAPI"


def get_secret(secret_id, region_name):
    try:
        sm_client = boto3.session.Session(region_name=region_name).client(
            "secretsmanager"
        )
        secret = sm_client.get_secret_value(SecretId=secret_id)
    except ClientError:
        current_app.logger.exception(f"Error getting secret {secret_id}")
        raise
    return secret


def check_api_key(client_id, input_api_key):
    secret = get_secret(SECRET_ID, REGION_NAME)

    try:
        secret_value = loads(secret["SecretString"])[client_id]
    except KeyError:
        current_app.logger.exception(f"Unknown client id {client_id}")
        return False

    try:
        api_key = loads(secret_value)["api_key"]
    except KeyError:
        current_app.logger.exception(
            f"Malformed secret {secret_value} for client {client_id}"
        )
        raise

    if api_key != input_api_key:
        current_app.logger.exception(f"Wrong api key input for client {client_id}")
        return False

    return True
