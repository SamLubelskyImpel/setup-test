"""Get Quiter data from FTP."""
import boto3
import logging
import os
from os import environ
from json import loads, dumps
from datetime import datetime, timedelta, timezone
from ftp_wrapper import FtpToS3


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
SNS_TOPIC_ARN = environ.get("CE_TOPIC")
SNS_CLIENT = boto3.client('sns')
SQS_CLIENT = boto3.client("sqs")
SQS_QUEUE_URLS = [
    environ.get("MERGE_REPAIR_ORDER_QUEUE"),
    environ.get("MERGE_APPOINTMENT_QUEUE"),
    environ.get("MERGE_VEHICLE_SALE_QUEUE")
]

FILE_PATTERNS = {
    "RepairOrder": ["RO"],
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "Appointment": ["APPT"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"]
}


def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/QuiterFTP"
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


def is_file_within_last_24_hours(ftp, file_name):
    file_time = ftp.sendcmd(f"MDTM {file_name}")[4:]
    file_datetime = datetime.strptime(file_time, "%Y%m%d%H%M%S")
    return datetime.now() - timedelta(days=1) <= file_datetime <= datetime.now()


def list_new_files(ftp, dealer_id):
    # Get current time and time 24 hours ago
    now = datetime.now(timezone.utc)
    last_24_hours = now - timedelta(days=1)
    ftp.cwd(dealer_id)
    logger.info(f"Changed to directory: {dealer_id}")
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


def process_files(ftp, dealer_id, found_files, s3_key):
    try:
        s3_client = boto3.client('s3')
        for file_type, file_name in found_files.items():
            local_file_path = f'/tmp/{file_name}'
            with open(local_file_path, 'wb') as f:
                ftp.retrbinary(f'RETR {file_name}', f.write)
            s3_client.upload_file(local_file_path, INTEGRATIONS_BUCKET, s3_key)
            os.remove(local_file_path)
    except ValueError as e:
        logger.error(f"Error uploading file {local_file_path} to S3: {e}")
        raise


def parse_data(data):
    """Parse and handle SQS Message."""
    logger.info(data)
    try:
        dealer_id = data["dealer_id"]
        end_dt = data["end_dt_str"]
        s3_date_path = datetime.strptime(end_dt, "%Y-%m-%dT%H:%M:%S").strftime("%Y/%m/%d")
        logger.info(f"S3 data path {s3_date_path}")
        host, user, password = get_ftp_credentials()
        ftp_session = FtpToS3(host=host, user=user, password=password)
        ftp = ftp_session.connect_to_ftp()

        if ftp:
            new_files = list_new_files(ftp, dealer_id)
            logger.info(f"New files found in the last 24 hours: {new_files}")
            if new_files:
                found_files = {}
                # Check for patterns for each file type within the recent files
                for file_type, patterns in FILE_PATTERNS.items():
                    for file in new_files:
                        matched = False
                        for pattern in patterns:
                            if pattern in file:
                                found_files[file_type] = file
                                matched = True
                                break
                        if matched:
                            break
                logger.info(f"Found files: {found_files}")
                # Ensure we have found a file for each file type
                if all(file_type in found_files for file_type in FILE_PATTERNS.keys()) and len(found_files) == len(FILE_PATTERNS):
                    logger.info("Success! All Quiter files are in place!")
                    current_date = datetime.now()
                    s3_key = f'quiter/landing_zone/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}'
                    process_files(ftp, dealer_id, found_files, s3_key)
                    for queue_url in SQS_QUEUE_URLS:
                        data = {
                            "dealer_id": dealer_id,
                            "s3_key": s3_key,
                        }
                        response = SQS_CLIENT.send_message(
                            QueueUrl=queue_url,
                            MessageBody=dumps(data)
                        )
                        # Verify that the message was sent successfully by checking the response
                        # TODO: Delete after testing
                        if 'MessageId' in response:
                            logging.info(f"Message sent successfully to {queue_url}, MessageId: {response['MessageId']}")
                        else:
                            logging.error(f"Failed to send message to {queue_url}")
                else:
                    message = f'QUITER: Not all required files are available on the FTP server for this dealer: {dealer_id}. Found files: {found_files}'
                    SNS_CLIENT.publish(
                            TopicArn=SNS_TOPIC_ARN,
                            Message=message
                        )
                    logger.error(message)
            else:
                logger.warning(f"No new files found in the last 24 hours for the dealer {dealer_id}.")
            ftp.quit()
        else:
            message = f'QUITER: Dealer {dealer_id} folder not found on the FTP server."'
            SNS_CLIENT.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message
                )
            logger.error(message)
    except Exception as e:
        logger.error(f"Error parsing data: {e}")
        raise


def lambda_handler(event, context):
    try:
        for event in event["Records"]:
            logger.info(f'Event: {event}')
            parse_data(loads(event["body"]))
    except Exception as e:
        logger.exception(f"Error running extract data lambda: {e}")
        raise
