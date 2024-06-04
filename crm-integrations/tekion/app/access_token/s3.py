import boto3
from os import environ

from .schemas import Token

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")

AUTH_BUCKET = f"auth-service-{REGION}-{'prod' if ENVIRONMENT == 'prod' else 'test'}"
PARTNER_KEY = environ.get("TEKION_PARTNER_KEY", "TEKION")

TEKION_AUTH_FILE_PATH = environ.get(
    "TEKION_AUTH_FILE_PATH",
    F"authorization_configs/crm_{PARTNER_KEY}_auth_config.json"
)


def get_token_from_s3() -> Token | None:
    content = boto3.client("s3").get_object(
        Bucket=AUTH_BUCKET, Key=TEKION_AUTH_FILE_PATH
    )["Body"].read()
    token = Token.model_validate_json(json_data=content) if content else None
    return token


def save_token_to_s3(token: Token) -> None:
    boto3.client("s3").put_object(
        Bucket=AUTH_BUCKET,
        Key=TEKION_AUTH_FILE_PATH,
        Body=token.model_dump_json().encode(),
    )
