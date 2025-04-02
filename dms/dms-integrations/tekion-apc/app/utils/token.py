from os import environ
import boto3
from json import loads, dumps
from datetime import datetime
from botocore.exceptions import ClientError

ENVIRONMENT = environ.get("ENVIRONMENT")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
REFRESH_TOKEN_LAMBDA = f"tekion-apc-dms-{ENVIRONMENT}-RefreshToken"


def get_token_from_s3():
    try:
        token = boto3.client("s3").get_object(
            Bucket=INTEGRATIONS_BUCKET,
            Key="tekion-apc/auth"
        )["Body"].read()

        token = loads(token)

        if token["expire_datetime"] <= str(datetime.utcnow()):
            return None

        return token["token"]
    except ClientError:
        return None


def save_token_to_s3(token: str, expire: datetime):
    boto3.client("s3").put_object(
        Bucket=INTEGRATIONS_BUCKET,
        Key="tekion-apc/auth",
        Body=dumps({
            "token": token,
            "expire_datetime": expire
        }, default=str)
    )


def invoke_refresh_token(dealer_id: str):
    boto3.client('lambda').invoke(
        FunctionName=REFRESH_TOKEN_LAMBDA,
        Payload=dumps({
            "Records": [
                {"body": dumps({"dealer_id": dealer_id})}
            ]
        }),
        InvocationType='Event'
    )
