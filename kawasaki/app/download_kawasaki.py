"""Download Kawasaki data for a dealer."""
from datetime import datetime, timezone
import logging
import requests
from uuid import uuid4
from json import loads
from os import environ
from urllib.parse import urlparse

import boto3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
S3_CLIENT = boto3.client("s3")
KAWASAKI_DATA_BUCKET = environ["KAWASAKI_DATA_BUCKET"]

default_suffix_mappings = {
    "dealerspike": "/feeds.asp?feed=GenericXMLFeed&version=2",
    "ari": "/unitinventory_univ.xml"
}


def download_kawasaki_file(web_provider, dealer_config):
    """Download Kawasaki file from url and upload to S3."""
    base_url = dealer_config["web_url"]
    if dealer_config.get("web_suffix", None):
        web_suffix = dealer_config["web_suffix"]
    else:
        web_suffix = default_suffix_mappings[web_provider]
    xml_url = f"{base_url}{web_suffix}"
    response = requests.get(xml_url)
    response.raise_for_status()
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    filename = f"{urlparse(base_url).netloc}_{now.strftime('%Y%m%d')}_{str(uuid4())}.xml"
    s3_key = f"raw/{web_provider}/{now.year}/{now.month}/{now.day}/{filename}"
    S3_CLIENT.put_object(
        Body=response.content,
        Bucket=KAWASAKI_DATA_BUCKET,
        Key=s3_key
    )
    logger.info(f"Uploaded {s3_key}")


def lambda_handler(event: dict, context: dict):
    """Download file for Kawasaki dealer."""
    try:
        for event in [e for e in event["Records"]]:
            message = loads(event["body"])
            download_kawasaki_file(message["web_provider"], message["dealer_config"])
    except Exception as e:
        logger.exception("Kawasaki file download failed.")
        raise e
