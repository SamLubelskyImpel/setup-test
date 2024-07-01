import logging
import boto3
from os import environ
from ftplib import FTP, error_perm, error_temp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FTP_FOLDER = environ.get("FTP_FOLDER", "test_tekion_dms")


class FtpToS3:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.s3_client = boto3.client("s3")

    def connect_to_ftp(self):
        try:
            ftp = FTP(self.host)
            ftp.login(self.user, self.password)
            return ftp
        except (error_perm, error_temp) as e:
            logger.error("Error connecting to FTP: %s", e)
            return None

    def check_folder_exists(self, ftp, dealer_id):
        try:
            directory = f"/{FTP_FOLDER}/{dealer_id}"
            ftp.cwd(directory)
            return True
        except error_perm as e:
            logger.info(f"Folder not found: {directory}. Error: {e}")
            return False
