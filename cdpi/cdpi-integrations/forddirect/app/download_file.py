import logging
import os
import  boto3
from json import loads
import paramiko
from io import StringIO, BytesIO
from datetime import datetime
import urllib.parse


IS_PROD = int(os.environ.get('IS_PROD'))
TOPIC_ARN = os.environ.get('ALERT_CLIENT_ENGINEERING_TOPIC')
BUCKET_NAME = os.environ.get('SHARED_BUCKET')

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())

s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')


def get_ssh_config():
    secret_name = f'{"prod" if IS_PROD else "test"}/CDPI/FD-SFTP'
    response = loads(secrets.get_secret_value(SecretId=secret_name)['SecretString'])
    return response['hostname'], response['username']


def get_ssh_pkey():
    secret_name = f'{"prod" if IS_PROD else "test"}/CDPI/FD-PKEY'
    return secrets.get_secret_value(SecretId=secret_name)['SecretString']

def validate_file(sftp, remote_file_path):
    file_info = sftp.stat(remote_file_path)
    file_size = int(file_info.st_size)

    logger.info(f"Attempting to download file of size {file_size} bytes")

    if file_size > 750000000:
        raise Exception(f"File {os.path.basename(remote_file_path)} is larger than 750 MB. Unable to download")
    return

def stream_file_in_chunks(sftp, remote_file_path, s3_file_path):
    """
    Downloads a file in chunks from SFTP to handle large file downloads more gracefully.
    """
    with BytesIO() as file_buffer:
        with sftp.file(remote_file_path, 'rb') as remote_file:
            s3.upload_fileobj(remote_file, BUCKET_NAME, s3_file_path)


def lambda_handler(event, context):
    logger.info(f'Record: {event}')

    try:
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
            validate_file(sftp, file_path)

            # Download the file from SFTP
            file_type = message["file_type"]
            current_date = datetime.now()
            partner_id = message["partner_id"]
            s3_path = f'download/{partner_id}/{file_type}/{current_date.year}/{current_date.month}/{current_date.day}/{os.path.basename(file_path)}'
            stream_file_in_chunks(sftp, file_path, s3_path)

            logger.info(f"File downloaded from FD: {file_path} to S3 {s3_path}")

            # Close SFTP connection
            sftp.close()
            transport.close()

    except Exception as e:
        logger.exception(f'Failed to upload file to S3: {e}')
        notify_client_engineering(e)
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
