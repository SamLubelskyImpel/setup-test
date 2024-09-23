"""Get Quiter data from FTP."""
import boto3
import logging
import os
from os import environ
from json import loads, dumps
from datetime import datetime, timedelta, timezone
from typing import Any
from ftp_wrapper import FtpToS3
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)


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


def list_new_files(ftp, dealer_id, end_dt):
    try:
        last_24_hours = end_dt - timedelta(days=1)
        new_files = []
        file_list = ftp.nlst()

        for file in file_list:
            try:
                file_modified_time_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()

                try:
                    file_modified_time = datetime.strptime(file_modified_time_str, "%Y%m%d%H%M%S.%f").replace(tzinfo=timezone.utc)
                except ValueError:
                    file_modified_time = datetime.strptime(file_modified_time_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

                if file_modified_time > last_24_hours:
                    new_files.append(file)
                    logger.info(f"File {file} is new (modified within the last 24 hours).")

            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")
                raise

        return new_files

    except Exception as e:
        logger.error(f"Error listing files in directory {dealer_id}: {e}")
        raise


def process_files(ftp, dealer_id, found_files, s3_key):
    try:
        s3_client = boto3.client('s3')
        for file_type, file_name in found_files.items():
            local_file_path = f'/tmp/{file_name}'
            with open(local_file_path, 'wb') as f:
                ftp.retrbinary(f'RETR {file_name}', f.write)
            s3_client.upload_file(local_file_path, INTEGRATIONS_BUCKET, f"{s3_key}/{file_name}")
            os.remove(local_file_path)
    except Exception as e:
        logger.error(f"Error uploading file {file_name} to S3: {e}")
        raise


def record_handler(record: SQSRecord) -> None:
    """Parse and handle SQS Message."""
    logger.info(f"Record: {record}")
    try:
        data = loads(record["body"])
        dealer_id = data["dealer_id"]
        end_dt_str = data["end_dt_str"]
        end_dt = datetime.strptime(end_dt_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        s3_date_path = datetime.strptime(end_dt_str, "%Y-%m-%dT%H:%M:%S").strftime("%Y/%-m/%-d")
        logger.info(f"S3 date path {s3_date_path}")
        host, user, password = get_ftp_credentials()
        ftp_session = FtpToS3(host=host, user=user, password=password)
        ftp = ftp_session.connect_to_ftp()
        folder_exists = ftp_session.check_folder_exists(ftp, dealer_id)
        if ftp and folder_exists:
            new_files = list_new_files(ftp, dealer_id, end_dt)
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
                    s3_key = f'quiter/landing_zone/{dealer_id}/{s3_date_path}'
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
                            logger.info(f"Message sent successfully to {queue_url}, MessageId: {response['MessageId']}")
                        else:
                            logger.error(f"Failed to send message to {queue_url}")
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
    except Exception as e:
        logger.error(f"Error parsing Quiter data: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Extract raw Quiter data and save it to S3."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Error running extract data lambda: {e}")
        raise
