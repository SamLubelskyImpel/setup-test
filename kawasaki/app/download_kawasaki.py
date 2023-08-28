"""Download Kawasaki data for a dealer."""
import logging
from datetime import datetime, timezone
from json import loads
from os import environ
from xml.etree import ElementTree

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
S3_CLIENT = boto3.client("s3")
KAWASAKI_DATA_BUCKET = environ["KAWASAKI_DATA_BUCKET"]

default_suffix_mappings = {
    "dealerspike": "/feeds.asp?feed=GenericXMLFeed&version=2",
    "ari": "/unitinventory_univ.xml",
}


def get_kawasaki_file(web_provider, dealer_config):
    """Given a web provider and dealer config, request dealer file."""
    base_url = dealer_config["web_url"]
    if dealer_config.get("web_suffix", None):
        # The dealer has a dealer specific suffix
        web_suffix = dealer_config["web_suffix"]
    else:
        # The dealer uses the default suffix for the given provider
        web_suffix = default_suffix_mappings.get(web_provider, "")
    xml_url = f"{base_url}{web_suffix}"
    # Downloads on lambda require user agent and Referer or else 403
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0"
    }
    if web_provider == "dealerspike":
        headers["Referer"] = "http://feeds-out.dealerspike.com/"
    response = requests.get(xml_url, headers=headers)
    response.raise_for_status()
    return response.content


def validate_xml_data(xml_string):
    """Given a string check if it is valid xml data that can be parsed."""
    if len(xml_string) <= 0:
        raise RuntimeError(f"No data found {xml_string}")
    ElementTree.fromstring(xml_string)


def download_kawasaki_file(web_provider, dealer_config):
    """Download Kawasaki file from url and upload to S3."""
    response_content = get_kawasaki_file(web_provider, dealer_config)
    validate_xml_data(response_content)

    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    filename = f"{web_provider}_{dealer_config['impel_id']}.xml"
    s3_key = f"raw/{web_provider}/{now.year}/{now.month}/{now.day}/{filename}"
    S3_CLIENT.put_object(Body=response_content, Bucket=KAWASAKI_DATA_BUCKET, Key=s3_key)
    logger.info(f"Uploaded {s3_key}")


def lambda_handler(event: dict, context: dict):
    """Download file for Kawasaki dealer."""
    for record in event["Records"]:
        try:
            message = loads(record["body"])
            download_kawasaki_file(message["web_provider"], message["dealer_config"])
        except Exception as exc:
            logger.exception("Kawasaki file download failed.")
            raise exc
