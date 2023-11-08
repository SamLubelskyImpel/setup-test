from os import environ
import logging
from json import loads
from tekion_wrapper import TekionWrapper
from utils.token import get_token_from_s3, save_token_to_s3


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    body = event["Records"][0]["body"]
    dealer_id = loads(body)["dealer_id"]

    token = get_token_from_s3()

    if token:
        logger.info("Token still valid")
        return {"statusCode": 200}

    logger.info("Token not valid, refreshing token")
    tekion = TekionWrapper(dealer_id)
    token, expire = tekion.authorize()
    logger.info("Expiry: " + str(expire))
    save_token_to_s3(token, expire)

    return {"statusCode": 200}

