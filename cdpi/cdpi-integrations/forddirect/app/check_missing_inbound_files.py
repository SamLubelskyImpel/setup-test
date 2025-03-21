import logging
from os import environ
from json import dumps
from uuid import uuid4
import boto3
from datetime import datetime, timedelta
from utils import send_missing_inbound_file_notification, send_alert_notification


from cdpi_orm.session_config import DBSession
from cdpi_orm.models.dealer import Dealer
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner

SHARED_BUCKET = environ.get("SHARED_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client('s3')


def get_dealer_integration_partner(dealer_id: int) -> dict:
    """Get the dealer integration partner based on the dealer id."""
    with DBSession() as session:
        dip = session.query(DealerIntegrationPartner).filter_by(dealer_id=dealer_id).first()
        if not dip:
            raise ValueError(f"Dealer {dealer_id} does not have an integration partner.")
        return dip


def lambda_handler(event, context):
    """Lambda handler for checking missing customer PII inbound files from Ford Direct."""

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info("Chekcing missing inbound files")
    
    try:
        date = datetime.now() - timedelta(days=2) # to make sure 24 hours have passed
        prefix = f"fd-pii-outbound/{date.year}/{date.month}/{date.day}"
        logger.info(f"Outbound Prefix: {prefix}")

        outbound_files = s3_client.list_objects_v2(Bucket=SHARED_BUCKET, Prefix=prefix)
        logger.info(f"Outbound Files: {outbound_files}")

        for file in outbound_files.get("Contents", []):
            key = file["Key"]

            if key.endswith("/"):
                continue

            logger.info(f"Found file: {key}")
            filename = key.split('impel_')[-1]
            dealer_id = filename.split('_')[0]

            dip = get_dealer_integration_partner(dealer_id)
            logger.info(f"Dealer Integration Partner: {dip}")

            inbound_prefix = f"fd-raw/pii_match/{dip.cdp_dealer_id}/{date.year}/{date.month}/{date.day}"
            logger.info(f"Inbound Prefix: {inbound_prefix}")

            inbound_files = s3_client.list_objects_v2(Bucket=SHARED_BUCKET, Prefix=inbound_prefix)
            logger.info(f"Inbound Files: {inbound_files}")

            inbound_file_exists = f'{inbound_prefix}/eid_pii_match_result_impel_{dealer_id}_{dip.cdp_dealer_id}' in inbound_files
            if inbound_file_exists:
                logger.info(f"File {key} has a matching inbound file on: {inbound_files.get('Contents', [])}")
            else:
                logger.info(f"Missing file: {key}")
                error_msg = f"The outbound file with key {key} has no matching inbound file on {SHARED_BUCKET}/{inbound_prefix}"
                send_missing_inbound_file_notification(error_msg)

        
    except Exception as e:
        logger.exception(f"Error invoking ford direct missing inbound files: {e}")
        send_alert_notification(request_id, "Ford Direct Missing Inbound Files", e)