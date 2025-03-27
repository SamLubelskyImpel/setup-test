import logging
from os import environ
from uuid import uuid4
import boto3
from datetime import datetime, timedelta
from utils import send_missing_files_notification, send_alert_notification

from cdpi_orm.session_config import DBSession
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner
from cdpi_orm.models.dealer import Dealer


SHARED_BUCKET = environ.get("SHARED_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client('s3')


def get_active_dealers() -> dict:
    """Get active dealers from the database."""

    with DBSession() as session:
        active_dealers = (session
                          .query(Dealer.id, DealerIntegrationPartner.cdp_dealer_id)
                          .join(DealerIntegrationPartner, DealerIntegrationPartner.dealer_id == Dealer.id)
                          .filter(DealerIntegrationPartner.is_active)
                          .all())
        logger.info(f'Active dealers: {active_dealers}')
        return active_dealers


def missing_weekly_file_error(dealers_obj, dealer_id, cdp_dealer_id, start_date, end_date):
    """Handle error for missing weekly file."""

    error_msg = f"Missing weekly file for dealer_id: {dealer_id} - cdp_dealer_id: {cdp_dealer_id} from {start_date} to {end_date}"
    logger.error(error_msg)
    dealers_obj[dealer_id] = {"cdp_dealer_id": cdp_dealer_id, "error_msg": error_msg}


def lambda_handler(event, context):
    """Lambda handler for checking missing consumer profile summary files from Ford Direct."""

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info("Checking missing consumer profile summary")
    
    try:
        active_dealers = get_active_dealers()
        logger.info(f"Active Dealers: {active_dealers}")
        today = datetime.now().date()
        date = today - timedelta(days=7)
        days_between_str = [((date + timedelta(days=x))).strftime('%Y%m%d') for x in range((today - date).days)]
        start_date = f"{days_between_str[0][:4]}-{days_between_str[0][4:6]}-{days_between_str[0][6:8]}"
        end_date = f"{days_between_str[-1][:4]}-{days_between_str[-1][4:6]}-{days_between_str[-1][6:8]}"
        dealers_obj = {}
        any_missing_files = False

        for dealer_id, cdp_dealer_id in active_dealers:
            logger.info(f"Dealer ID: {dealer_id} - CDP Dealer ID: {cdp_dealer_id}")

            raw_files_prefix = f"fd-raw/consumer_profile_summary/{cdp_dealer_id}"

            folder_content = s3_client.list_objects_v2(Bucket=SHARED_BUCKET, Prefix=raw_files_prefix)
            logger.info(f"Folder_content: {folder_content}")

            files = folder_content.get("Contents")
            logger.info(f"Files: {files}")

            if not files:
                any_missing_files = True
                missing_weekly_file_error(dealers_obj, dealer_id, cdp_dealer_id, start_date, end_date)
                continue
            
            files_str = str(files)
            
            for day_str in days_between_str:
                year,month,day = day_str[:4],day_str[4:6],day_str[6:8]
                file_key = f"{raw_files_prefix}/{year}/{month}/{day}/consumerprofilesummary_impel_{cdp_dealer_id}_{day_str}"

                if file_key in files_str:
                    logger.info(f"File {file_key} exists for date: {f'{year}-{month}-{day}'}")
                    break
            else:
                any_missing_files = True
                missing_weekly_file_error(dealers_obj, dealer_id, cdp_dealer_id, start_date, end_date)

        if any_missing_files:
            logger.info(f"Missing files dict: {dealers_obj}")
            send_missing_files_notification(f"CDPI FORD DIRECT: Missing Consumer Profile Summary Alert", dealers_obj)
        
        logger.info("No missing consumer profile summary files found.")
    
    except Exception as e:
        logger.exception(f"Error invoking ford direct missing consumer profile summary: {e}")
        send_alert_notification(request_id, "Ford Direct Missing Consumer Profile Summary", e)