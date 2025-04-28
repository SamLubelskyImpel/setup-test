import logging
from os import environ
from uuid import uuid4
from datetime import datetime, timedelta
from utils import get_sftp_secrets, connect_sftp_server, send_alert_notification, send_alert_missing_inventory_files
from rds_instance import RDSInstance

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def order_dealers_by_product(active_dealers, ordered_dealers):
    """Order active dealers by product and store their SFTP keys."""

    for dealer in active_dealers:
        dealer_id, impel_dealer_id, metadata = dealer[0], dealer[1], dealer[2]
        logger.info(f"Processing dealer_id: {dealer_id} - impel_dealer_id: {impel_dealer_id} - metadata: {metadata}")
        
        if metadata:
            syndications = metadata.get("syndications", {})
            for product, extra_data in syndications.items():
                if extra_data.get("active", False):
                    if not product in ordered_dealers:
                        ordered_dealers[product] = {"dealers": [], "sftp_key": ""}
                    if not ordered_dealers[product]["sftp_key"]:
                        ordered_dealers[product]["sftp_key"] = extra_data.get("sftp_key", "")
                    
                    dealer_data = {"dealer_id": dealer_id, "impel_dealer_id": impel_dealer_id}
                    ordered_dealers[product]["dealers"].append(dealer_data)

    logger.info(f"Active dealers by product: {ordered_dealers}")


def lambda_handler(event, context):
    """Lambda handler for checking missing inventory files."""

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")
    logger.info("Checking missing inbound files")
    
    error_report = []
    ordered_dealers = {}

    try:
        rds_instance = RDSInstance()
        active_dealers = rds_instance.get_active_dealers()
        order_dealers_by_product(active_dealers, ordered_dealers)

        for product,dealers_data in ordered_dealers.items():
            logger.info(f"Checking for files on {dealers_data['sftp_key']}...")
                
            hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", dealers_data['sftp_key'])
            
            with connect_sftp_server(hostname, port, username, password) as sftp:
                logger.info(f"Connected to {dealers_data['sftp_key']} SFTP server.")

                for dealer in dealers_data["dealers"]:
                    dealer_id, impel_dealer_id = dealer["dealer_id"], dealer["impel_dealer_id"]
                    logger.info(f"Checking dealer_id: {dealer_id} - impel_dealer_id: {impel_dealer_id}")

                    error_msg = ""
                    filename = f"{impel_dealer_id}.csv"
                    logger.info(f"Checking for file: {filename} on {dealers_data['sftp_key']}")

                    try:
                        file_attr = sftp.stat(filename)
                        modified_timestamp = file_attr.st_mtime
                        modified_datetime = datetime.fromtimestamp(modified_timestamp)

                        logger.info(f"File {filename} exists. Last modified: {modified_datetime.isoformat()}")

                        if modified_datetime < datetime.now() - timedelta(hours=24):
                            error_msg = f"File {filename} is older than 24 hours, Dealer is outdated on {dealers_data['sftp_key']}."
                            logger.info(error_msg)

                    except FileNotFoundError:
                        error_msg = f"File {filename} does not exist on {dealers_data['sftp_key']}."
                        logger.info(error_msg)
                    
                    if error_msg:
                        error_report.append({
                            "dealer_id": dealer_id,
                            "impel_dealer_id": impel_dealer_id,
                            "product": product,
                            "error_msg": error_msg
                        })

        if error_report:
            logger.info(f"Missing files dict: {error_report}")
            send_alert_missing_inventory_files(request_id, error_report)

    except Exception as e:
        logger.exception(f"Error invoking inventory files check: {e}")
        send_alert_notification(request_id, "Inventory files check", e)