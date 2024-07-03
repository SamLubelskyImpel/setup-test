import json
import logging
from datetime import datetime
from typing import Optional

import boto3

from .envs import INTEGRATIONS_BUCKET, TOKEN_FILE, LOG_LEVEL, SECRET_KEY, CRM_INTEGRATION_SECRETS_ID
from .schemas import Token, TekionCredentials

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

def get_credentials_from_secrets() -> TekionCredentials:
    logger.debug(
        f"Getting Tekion credentials from secrets manager: {CRM_INTEGRATION_SECRETS_ID}"
    )
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=CRM_INTEGRATION_SECRETS_ID)
    content = json.loads(response["SecretString"])
    partner_content = content[SECRET_KEY]

    if isinstance(partner_content, str):
        partner_content = json.loads(partner_content)

    return TekionCredentials(**partner_content)


def get_token_from_s3() -> Optional[Token]:
    logger.info(
        "Getting token from S3: bucket=%s, key=%s",
        INTEGRATIONS_BUCKET, TOKEN_FILE
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
        else:
            logger.error("ClientError: %s", err)
            raise
