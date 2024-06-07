import json
from datetime import datetime

import boto3
from aws_lambda_powertools import Logger

from .envs import INTEGRATIONS_BUCKET, TOKEN_FILE
from .schemas import Token

logger = Logger()


def get_token_from_s3() -> Token | None:
    logger.info(
        "Fetching token from S3",
        extra={"bucket": INTEGRATIONS_BUCKET, "key": TOKEN_FILE}
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
    logger.info(
        "Saving token to S3",
        extra={"bucket": INTEGRATIONS_BUCKET, "key": TOKEN_FILE}
    )
    boto3.client("s3").put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key=TOKEN_FILE,
        Body=token.as_json().encode(),
    )
