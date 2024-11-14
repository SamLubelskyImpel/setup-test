"""Get historical data from FTP."""
import boto3
import logging
from os import environ
from json import loads
from datetime import datetime, timedelta, timezone
from ftp_wrapper import FtpToS3

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
SNS_TOPIC_ARN = environ.get("CE_TOPIC")
FTP_FOLDER = environ.get("FTP_FOLDER")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client('s3')


def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/TekionFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        secret_data = loads(secret["SecretString"])
        host = secret_data.get("host")
        user = secret_data.get("user")
        password = secret_data.get("password")
        return host, user, password
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise


def list_new_files(ftp):
    # Get current time and time 24 hours ago
    now = datetime.now(timezone.utc)
    last_24_hours = now - timedelta(days=1)
    ftp.cwd(FTP_FOLDER)
    logger.info(f"Changed to directory: {FTP_FOLDER}")
    new_files = []
    for file in ftp.nlst():
        file_modified_time_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
        try:
            file_modified_time = datetime.strptime(file_modified_time_str, "%Y%m%d%H%M%S.%f").replace(tzinfo=timezone.utc)
        except ValueError:
            file_modified_time = datetime.strptime(file_modified_time_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        if file_modified_time > last_24_hours:
            new_files.append(file)
    return new_files


def upload_file_to_s3(local_file, s3_key):
    try:
        with open(local_file, 'rb') as data:
            s3_client.put_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=data)
            logger.info(f"File {local_file} uploaded to S3 as {s3_key}.")
    except ValueError as e:
        logger.error(f"Error uploading file {local_file} to S3: {e}")
        raise


def process_file(file, ftp, dealer_id, s3_date_path):
    try:
        if file.startswith(f"{dealer_id}_"):
            local_file = f"/tmp/{file}"
            ftp.retrbinary(f"RETR {file}", open(local_file, 'wb').write)

            if any(keyword in file for keyword in ["RepairOrder", "RO", "SERVICE"]):
                s3_key = f"tekion-apc/historical/repair_order/{dealer_id}/{s3_date_path}/{file}"
            elif any(keyword in file for keyword in ["VehicleSales", "VSH", "SALES"]):
                s3_key = f"tekion-apc/historical/fi_closed_deal/{dealer_id}/{s3_date_path}/{file}"
            else:
                raise ValueError(f"Unknown file type for file {file}")

            upload_file_to_s3(local_file, s3_key)
    except Exception as e:
        logger.error(f"Error processing file {file}: {e}")
        raise


def parse_data(data):
    """Parse and handle SQS Message."""
    logger.info(data)
    try:
        dealer_id = data["dealer_id"]
        end_dt = data["end_dt_str"]
        s3_date_path = datetime.strptime(end_dt, "%Y-%m-%dT%H:%M:%S").strftime("%Y/%m/%d")
        host, user, password = get_ftp_credentials()
        ftp_session = FtpToS3(host=host, user=user, password=password)
        ftp = ftp_session.connect_to_ftp()

        if ftp:
            new_files = list_new_files(ftp)
            logger.info(f"New files found in the last 24 hours: {new_files}")
            if new_files:
                for file in new_files:
                    try:
                        process_file(file, ftp, dealer_id, s3_date_path)
                    except Exception as e:
                        logger.error(f"Error processing file {file}: {e}")
                        raise
            else:
                logger.info(f"No new files found in the last 24 hours for dealer {dealer_id}.")
        else:
            logger.info(f"Dealer {dealer_id} folder not found in FTP server.")
    except Exception as e:
        logger.error(f"Error parsing data: {e}")
        raise


def lambda_handler(event, context):
    try:
        for event in event["Records"]:
            parse_data(loads(event["body"]))
    except Exception as e:
        logger.exception(f"Error running historical data lambda: {e}")
        raise
