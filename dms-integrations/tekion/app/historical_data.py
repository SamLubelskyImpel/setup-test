"""Get historical data from FTP."""
import boto3
import logging
from os import environ
from json import loads
from datetime import datetime, timedelta
from ftp_wrapper import FtpToS3


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
SNS_TOPIC_ARN = environ.get("CE_TOPIC")
SNS_CLIENT = boto3.client('sns')


def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/TekionFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise


def parse_data(data):
    """Parse and handle SQS Message."""
    logger.info(data)
    dealer_id = data["dealer_id"]
    ftp_session = FtpToS3(**get_ftp_credentials())
    ftp = ftp_session.connect_to_ftp()
    s3_client = boto3.client('s3')

    if ftp:
        if ftp_session.check_folder_exists(ftp, dealer_id):
            logger.info(f"Dealer {dealer_id} folder found in FTP server.")
            current_directory = ftp.pwd()
            logger.info(f"Current directory: {current_directory}")

            # Get current time and time 24 hours ago
            now = datetime.now()
            last_24_hours = now - timedelta(days=1)

            # List files and check their modification time
            new_files = []
            for file in ftp.nlst():
                file_modified_time = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                try:
                    file_modified_time = datetime.strptime(file_modified_time, "%Y%m%d%H%M%S.%f")
                except ValueError:
                    file_modified_time = datetime.strptime(file_modified_time, "%Y%m%d%H%M%S")

                if file_modified_time > last_24_hours:
                    new_files.append(file)

            logger.info(f"New files found in the last 24 hours: {new_files}")
            if new_files:
                for file in new_files:
                    try:
                        local_file = f"/tmp/{file}"
                        ftp.retrbinary(f"RETR {file}", open(local_file, 'wb').write)

                        # Determine S3 key based on file name
                        if "RepairOrder" in file:
                            s3_key = f"tekion/historical/repair_order/{dealer_id}/{file}"
                        elif "VehicleSales" in file:
                            s3_key = f"tekion/historical/fi_closed_deal/{dealer_id}/{file}"
                        else:
                            raise ValueError(f"Unknown file type for file {file}")

                        with open(local_file, 'rb') as data:
                            s3_client.put_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=data)

                        logger.info(f"File {file} uploaded to S3 as {s3_key}.")
                    except ValueError as e:
                        logger.error(f"Error transferring file {file} to S3: {e}")
            else:
                logger.info(f"No new files found in the last 24 hours for dealer {dealer_id}.")
        else:
            logger.info(f"Dealer {dealer_id} folder not found in FTP server.")


def lambda_handler(event, context):
    try:
        for event in [e for e in event["Records"]]:
            parse_data(loads(event["body"]))
    except Exception:
        logger.exception("Error running historical data lambda")
        raise
