import logging
import boto3
from ftplib import FTP, error_perm, error_temp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
            raise