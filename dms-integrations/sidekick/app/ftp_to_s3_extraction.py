import os
import logging
import boto3
from ftplib import FTP, error_perm, error_temp
from utils.csv_formatter import CsvFormatter

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FtpToS3:
    def __init__(self, ftp_host, ftp_user, ftp_password, aws_profile):
        self.host = ftp_host
        self.user = ftp_user
        self.password = ftp_password
        self.s3_client = boto3.client("s3", profile_name=aws_profile)

    def connect_to_ftp(self):
        try:
            ftp = FTP(self.host)
            ftp.login(self.user, self.password)
            return ftp
        except (error_perm, error_temp) as e:
            logger.error("Error connecting to FTP: %s", e)
            return None

    def download_from_ftp(self, filename, local_file_path, remote_directory):
        if self.connected_ftp is None:
            logger.error("FTP connection is not established.")
            return False
        try:
            self.connected_ftp.cwd(remote_directory)
            with open(local_file_path, 'wb') as local_file:
                self.connected_ftp.retrbinary(f'RETR {filename}', local_file.write)
            logger.info("File %s downloaded successfully.", filename)
            return True
        except (error_perm, error_temp) as e:
            logger.error("Error downloading file %s from FTP: %s", filename, e)
            return False

    def transfer_data_to_s3(self, bucket_name, remote_filename, local_filepath):
        try:
            with open(local_filepath, 'rb') as file_data:
                self.s3_client.upload_fileobj(
                    Fileobj=file_data,
                    Bucket=bucket_name,
                    Key=remote_filename,
                )
            logger.info("File %s uploaded to S3 bucket %s as %s", local_filepath, bucket_name, remote_filename)
            return True
        except boto3.exceptions.S3UploadFailedError as e:
            logger.error("Error uploading file to S3: %s", e)
            return False

    def transfer_csv_from_ftp_to_s3(self, filename, remote_directory, local_file_path, bucket_name, dealer_id):
        self.connected_ftp = self.connect_to_ftp()
        if self.connected_ftp:
            local_file_full_path = os.path.join("utils", local_file_path)
            if self.download_from_ftp(filename, local_file_full_path, remote_directory):
                csv_formatter = CsvFormatter(dealer_id=dealer_id)
                csv_formatter.read_local_file(file_name=local_file_full_path)
                csv_formatter.write_csv_file(file_name=local_file_full_path)

                success = self.transfer_data_to_s3(
                    bucket_name=bucket_name,
                    remote_filename=os.path.join("sidekick", remote_directory, filename),
                    local_filepath=local_file_full_path
                )
                if success:
                    os.remove(local_file_full_path)  # Remove local file after successful transfer
        else:
            logger.error("Problem with connection to the FTP server")

