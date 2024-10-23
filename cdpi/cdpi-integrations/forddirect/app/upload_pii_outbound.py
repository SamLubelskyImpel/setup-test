import logging
import os
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from typing import Any
import  boto3
from json import loads
import paramiko
from io import StringIO
import urllib.parse


IS_PROD = int(os.environ.get('IS_PROD'))

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


def record_handler(record):
    logger.info(f'Record: {record}')

    try:
        event = loads(record.body)
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(file_key)

        # Download the S3 file to /tmp (Lambda's temp storage)
        download_path = f'/tmp/{os.path.basename(decoded_key)}'
        s3.download_file(bucket_name, decoded_key, download_path)

        sftp_host, sftp_username = get_ssh_config()
        private_key_content = get_ssh_pkey()

        # Load private key using Paramiko
        private_key = paramiko.RSAKey.from_private_key(StringIO(private_key_content))

        # Establish SFTP connection
        transport = paramiko.Transport((sftp_host, 22))
        transport.connect(username=sftp_username, pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Define the SFTP target directory and file path
        sftp_target_directory = 'input/pii_match' if IS_PROD else 'input/pii_match_test'
        sftp_target_path = os.path.join(sftp_target_directory, os.path.basename(decoded_key))

        # Upload the file to SFTP
        sftp.put(download_path, sftp_target_path)

        logger.info(f"File uploaded to FD: {sftp_target_path}")

        # Close SFTP connection
        sftp.close()
        transport.close()
    except:
        logger.exception(f'Failed to upload file to FD')
        raise


def lambda_handler(event: Any, context: Any):
    """Lambda function entry point for processing SQS messages."""
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
    except:
        logger.exception(f"Error processing records")
        raise