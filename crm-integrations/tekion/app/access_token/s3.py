from os import environ

import boto3
from aws_lambda_powertools import Logger

from .schemas import Token

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")

AUTH_BUCKET = f"auth-service-{REGION}-{'prod' if ENVIRONMENT == 'prod' else 'test'}"
PARTNER_KEY = environ.get("TEKION_PARTNER_KEY", "TEKION")

TEKION_AUTH_FILE_PATH = environ.get(
    "TEKION_AUTH_FILE_PATH",
    F"authorization_configs/crm_{PARTNER_KEY}_auth_config.json"
)

logger = Logger()


def get_token_from_s3() -> Token | None:
    # logger.debug(
    #     "Fetching token from S3",
    #     extra={
    #         "bucket": AUTH_BUCKET,
    #         "key": TEKION_AUTH_FILE_PATH,
    #     }
    # )
    # content = boto3.client("s3").get_object(
    #     Bucket=AUTH_BUCKET, Key=TEKION_AUTH_FILE_PATH
    # )["Body"].read()
    # token = Token.model_validate_json(json_data=content) if content else None
    # return token
    return None


def save_token_to_s3(token: Token) -> None:
    logger.debug(
        "Saving token to S3",
        extra={
            "bucket": AUTH_BUCKET,
            "key": TEKION_AUTH_FILE_PATH,
        }
    )
    boto3.client("s3").put_object(
        Bucket=AUTH_BUCKET,
        Key=TEKION_AUTH_FILE_PATH,
        Body=token.model_dump_json().encode(),
    )
