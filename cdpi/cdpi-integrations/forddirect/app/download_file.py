import logging
import os
import boto3
from json import loads
import paramiko
from io import StringIO
from datetime import datetime
import urllib.parse

ENVIRONMENT = os.environ.get('ENVIRONMENT')
TOPIC_ARN = os.environ.get('ALERT_CLIENT_ENGINEERING_TOPIC')
BUCKET_NAME = os.environ.get('SHARED_BUCKET')

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')


class UnknownFileError(Exception):
    pass


def get_ssh_config():
    secret_name = f'{"prod" if ENVIRONMENT == "prod" else "test"}/CDPI/FD-SFTP'
    response = loads(secrets.get_secret_value(SecretId=secret_name)['SecretString'])
    return response['hostname'], response['username']


def get_ssh_pkey():
    secret_name = f'{"prod" if ENVIRONMENT == "prod" else "test"}/CDPI/FD-PKEY'
    return secrets.get_secret_value(SecretId=secret_name)['SecretString']


def validate_file(sftp, remote_file_path):
    try:
        file_info = sftp.stat(remote_file_path)
        file_size = int(file_info.st_size)
    except FileNotFoundError:
        raise Exception(f"File {os.path.basename(remote_file_path)} not found on SFTP server")

    logger.info(f"Attempting to download file of size {file_size} bytes")

    if file_size > 750000000:
        raise Exception(f"File {os.path.basename(remote_file_path)} is larger than 750 MB. Unable to download")
    return


def stream_file_to_s3(sftp, remote_file_path, s3_file_path):
    """Downloads a file from SFTP to s3 bucket."""
    with sftp.file(remote_file_path, 'rb') as remote_file:
        s3.upload_fileobj(remote_file, BUCKET_NAME, s3_file_path)


def extract_dealer_id(file_path):
    if 'temp-processing' in file_path:
        raise UnknownFileError(f"File is not meant for processing: {file_path}")
    if 'dealer_attributes' in file_path:
        return None
    if 'consumer_profile_summary_enterprise' in file_path:
        return file_path.split('consumer_profile_summary_enterprise/')[1].split('/')[0]
    if 'consumer_profile_summary' in file_path:
        return file_path.split('consumer_profile_summary/')[1].split('/')[0]
    if 'consumer_signal_detail' in file_path:
        return file_path.split('consumer_signal_detail/')[1].split('/')[0]
    if 'pii-match' in file_path or 'pii_match' in file_path:
        return file_path.split('match/')[1].split('/')[0]
    if 'inventory' in file_path:
        return file_path.split('inventory/')[1].split('/')[0]

    raise Exception(f"Unable to extract dealer ID from file path: {file_path}")


def lambda_handler(event, context):
    """Download a file from SFTP and upload to S3."""
    try:
        logger.info(f'Record: {event}')

        for record in event["Records"]:
            message = loads(record["body"])

            file_path = message["file_path"]
            decoded_key = urllib.parse.unquote(file_path)

            sftp_host, sftp_username = get_ssh_config()
            private_key_content = get_ssh_pkey()

            # Load private key using Paramiko
            private_key = paramiko.RSAKey.from_private_key(StringIO(private_key_content))

            # Establish SFTP connection
            transport = paramiko.Transport((sftp_host, 22))
            transport.connect(username=sftp_username, pkey=private_key)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # Validate file is within acceptable size range
            validate_file(sftp, decoded_key)

            # Download the file from SFTP
            file_type = message["file_type"]
            current_date = datetime.now()

            cdp_dealer_id = extract_dealer_id(decoded_key)
            if cdp_dealer_id:
                s3_path = f'fd-raw/{file_type}/{cdp_dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{os.path.basename(decoded_key)}'
            else:
                s3_path = f'fd-raw/{file_type}/{current_date.year}/{current_date.month}/{current_date.day}/{os.path.basename(decoded_key)}'

            stream_file_to_s3(sftp, decoded_key, s3_path)
            logger.info(f"File downloaded from FD: {decoded_key} to S3 {s3_path}")

            # Close SFTP connection
            sftp.close()
            transport.close()

    except UnknownFileError as e:
        logger.warning(f"File not meant for processing: {e}")
        return

    except Exception as e:
        message = f"SFTP FD File Download Failed: {e}"
        logger.exception(message)
        notify_client_engineering(message)
        raise


def notify_client_engineering(error_message):
    """Send a notification to the client engineering SNS topic."""
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="FordDirect Download Lambda Error",
        Message=str(error_message),
    )
    return
