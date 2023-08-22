"""Download Kawasaki data for a dealer."""
import logging
from datetime import datetime, timezone
from json import loads
from os import environ
from urllib.parse import urlparse
from uuid import uuid4
from xml.etree import ElementTree
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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


# https://www.peterbe.com/plog/best-practice-with-retries-with-requests
def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    """Request with preset best values."""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_kawasaki_file(web_provider, dealer_config):
    """Given a web provider and dealer config, request dealer file."""
    base_url = dealer_config["web_url"]
    if dealer_config.get("web_suffix", None):
        web_suffix = dealer_config["web_suffix"]
    else:
        web_suffix = default_suffix_mappings[web_provider]
    xml_url = f"{base_url}{web_suffix}"
    session = requests.Session()
    response = requests_retry_session(session=session).get(xml_url)
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
    filename = f"{web_provider}_{urlparse(dealer_config['web_url']).netloc}_{now.strftime('%Y%m%d')}_{str(uuid4())}.xml"
    s3_key = f"raw/{web_provider}/{now.year}/{now.month}/{now.day}/{filename}"
    S3_CLIENT.put_object(Body=response_content, Bucket=KAWASAKI_DATA_BUCKET, Key=s3_key)
    logger.info(f"Uploaded {s3_key}")


def lambda_handler(event: dict, context: dict):
    """Download file for Kawasaki dealer."""
    for event in [e for e in event["Records"]]:
        try:
            message = loads(event["body"])
            download_kawasaki_file(message["web_provider"], message["dealer_config"])
        except Exception as exc:
            logger.exception("Kawasaki file download failed.")
            raise exc
