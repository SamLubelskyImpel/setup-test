import logging
import boto3
import paramiko

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FtpToS3:
    def __init__(self, host, user, password, port):
        self.host = host
        self.user = user
        self.password = password
        self.s3_client = boto3.client("s3")
        self.port = port

    def connect_to_ftp(self):
        try:
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(username=self.user, password=self.password)
            ftp = paramiko.SFTPClient.from_transport(transport)
            logger.info("SFTP connection successful")
            return ftp
        except paramiko.AuthenticationException:
            print("Authentication failed. Check your username/password.")
            raise
        except paramiko.SSHException as e:
            print(f"SSH error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error connecting to SFTP: {e}")
            raise
