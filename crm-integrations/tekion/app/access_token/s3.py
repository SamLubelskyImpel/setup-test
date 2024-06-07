import json
import logging

import boto3

from .envs import INTEGRATIONS_BUCKET, TOKEN_FILE, LOGLEVEL
from .schemas import Token

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


def get_token_from_s3() -> Token | None:
    logger.info(
        "Fetching token from S3: %s/%s", INTEGRATIONS_BUCKET, TOKEN_FILE
    )
    content = boto3.client("s3").get_object(
        Bucket=INTEGRATIONS_BUCKET, Key=TOKEN_FILE
    )["Body"].read()
    token = Token(**json.loads(content)) if content else None
    return token


def save_token_to_s3(token: Token) -> None:
    logger.debug(
        "Saving token to S3: %s/%s", INTEGRATIONS_BUCKET, TOKEN_FILE,
    )
    boto3.client("s3").put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=TOKEN_FILE,
        Body=token.as_json().encode(),
    )
