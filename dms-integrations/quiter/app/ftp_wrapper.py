import logging
import boto3
from os import environ
from ftplib import FTP, error_perm, error_temp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SNS_TOPIC_ARN = environ.get("CE_TOPIC")
SNS_CLIENT = boto3.client('sns')


class FtpToS3:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    def connect_to_ftp(self):
        try:
            ftp = FTP(self.host)
            ftp.login(self.user, self.password)
            return ftp
        except (error_perm, error_temp) as e:
            logger.error("Error connecting to FTP: %s", e)
            raise

    def check_folder_exists(self, ftp, dealer_id):
        folder_path = f'{dealer_id}'
        try:
            ftp.cwd(folder_path)
            return True
        except error_perm:
            message = f'QUITER: Dealer {dealer_id} folder not found on the FTP server.'
            SNS_CLIENT.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message
            )
            logger.error(message)
            return False