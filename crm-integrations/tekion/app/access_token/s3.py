import json
import logging
from datetime import datetime

import boto3

from .envs import INTEGRATIONS_BUCKET, TOKEN_FILE, LOGLEVEL
from .schemas import Token

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


def get_token_from_s3() -> Token | None:
    logger.info(
        "Fetching token from S3: %s/%s", INTEGRATIONS_BUCKET, TOKEN_FILE
    )
    client = boto3.client("s3")
    try:
        content = client.get_object(
            Bucket=INTEGRATIONS_BUCKET, Key=TOKEN_FILE
        )["Body"].read()

        if not content:
            logger.info("Empty token file found in S3")
            return None

        logger.info("Token file found in S3")

        data = json.loads(content)
        token = Token(
            token=data["token"],
            expires_in_seconds=data["expires_in_seconds"],
            created_at=datetime.fromisoformat(data["created_at"]),
            token_type=data["token_type"],
        )

        return token

    except client.exceptions.NoSuchKey:
        logger.info("Token file not found in S3")
        return None

    except client.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            logger.info("Token file not found in S3")
            return None


def save_token_to_s3(token: Token) -> None:
    logger.debug(
        "Saving token to S3: %s/%s", INTEGRATIONS_BUCKET, TOKEN_FILE,
    )
    boto3.client("s3").put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=TOKEN_FILE,
        Body=token.as_json().encode(),
    )
