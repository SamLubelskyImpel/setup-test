import io
import csv
import logging
import boto3
from ftplib import FTP, error_perm, error_temp

# Setup basic logging
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
            return None

    def transfer_file_from_ftp_to_s3(self, filename, date_path, bucket_name, parent_store, child_store):
        self.connected_ftp = self.connect_to_ftp()
        if self.connected_ftp:
            self.connected_ftp.cwd(f"{parent_store}/{date_path}")
            buffer = io.BytesIO()
            try:
                self.connected_ftp.retrbinary(f"RETR {filename}", buffer.write)
                buffer.seek(0)  # Rewind the buffer to read its content
                
                # Process CSV content, filtering out rows without the matching Site Id
                text_buffer = io.StringIO(buffer.getvalue().decode('utf-8'))
                reader = csv.DictReader(text_buffer, delimiter="|")
                fieldnames = reader.fieldnames

                # Use StringIO to accumulate filtered CSV content
                output_string_buffer = io.StringIO()
                writer = csv.DictWriter(output_string_buffer, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in reader:
                    if row.get('Site ID') == f"{parent_store}-{child_store}":
                        writer.writerow(row)
                
                # Encode StringIO content and write to BytesIO
                filtered_csv_content = output_string_buffer.getvalue().encode('utf-8')
                filtered_csv = io.BytesIO(filtered_csv_content)
                # Ensure the S3 key has a .csv extension
                s3_key = f"sidekick/{parent_store}/{child_store}/{date_path}/{filename}"

                # Upload directly from the filtered buffer to S3
                self.s3_client.upload_fileobj(
                    Fileobj=filtered_csv,
                    Bucket=bucket_name,
                    Key=s3_key,
                )
                logger.info("Filtered file %s uploaded to S3 bucket %s", filename, bucket_name)
                return True
            except (error_perm, error_temp, boto3.exceptions.S3UploadFailedError) as e:
                logger.error("Error during FTP to S3 transfer: %s", e)
                return False
        else:
            logger.error("Problem with connection to the FTP server")
