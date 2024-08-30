import io
import csv
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
        self.s3_client = boto3.client("s3")

    def connect_to_ftp(self):
        try:
            ftp = FTP(self.host)
            ftp.login(self.user, self.password)
            return ftp
        except (error_perm, error_temp) as e:
            logger.error("Error connecting to FTP: %s", e)
            return None

    def check_directory_exists(self, directory):
        try:
            self.connected_ftp.cwd(directory)
            return True
        except error_perm as e:
            logger.info(f"Directory {directory} does not exist. Error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error while checking directory {directory}: {e}")
            return False

    def alert_topic(self, subject, message):
        SNS_CLIENT.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=subject
        )

    def process_and_upload_file(self, filename, directory, s3_key, bucket_name, parent_store, child_store):
        try:
            # Proceed with file retrieval and processing
            buffer = io.BytesIO()
            self.connected_ftp.cwd(directory)
            self.connected_ftp.retrbinary(f"RETR {filename}", buffer.write)
            buffer.seek(0)

            # Process CSV content, filtering out rows without the matching Site ID
            text_buffer = io.StringIO(buffer.getvalue().decode('utf-8'))
            reader = csv.DictReader(text_buffer, delimiter="|")
            fieldnames = reader.fieldnames

            output_string_buffer = io.StringIO()
            writer = csv.DictWriter(output_string_buffer, fieldnames=fieldnames, delimiter="|")
            writer.writeheader()

            for row in reader:
                if row.get('Site ID') == f"{parent_store}-{child_store}":
                    writer.writerow(row)

            # Prepare filtered data for upload
            filtered_csv_content = output_string_buffer.getvalue().encode('utf-8')
            filtered_csv = io.BytesIO(filtered_csv_content)

            # Upload directly from the filtered buffer to S3
            self.s3_client.upload_fileobj(
                Fileobj=filtered_csv,
                Bucket=bucket_name,
                Key=s3_key,
            )
            logger.info("Filtered file %s uploaded to S3 bucket %s", filename, bucket_name)
        except Exception as e:
            logger.error(f"Error processing file {filename}: {e}")
            raise

    def transfer_file_from_ftp_to_s3(self, date_paths, bucket_name, parent_store, child_store):
        try:
            self.connected_ftp = self.connect_to_ftp()
            if not self.connected_ftp:
                raise ConnectionError("Problem with connection to the FTP server")

            # Flag to track if any daily data is found (for daily files only)
            daily_data_found = False

            for date_path in date_paths:
                directory = f"/{parent_store}/{date_path}"

                if not self.check_directory_exists(directory):
                    continue  # Try the next directory if this one does not exist

                try:
                    files = self.connected_ftp.nlst()
                    if not files:
                        logger.info(f"No files found in the directory: {directory}")
                        continue  # Try the next directory if no files found

                    for filename in files:
                        if 'historical' in date_path:
                            s3_key = f"sidekick/repair_order/historical/{parent_store}/{child_store}/{date_path}/{filename}"
                        else:
                            s3_key = f"sidekick/repair_order/{parent_store}/{child_store}/{date_path}/{filename}"
                            daily_data_found = True

                        self.process_and_upload_file(filename, directory, s3_key, bucket_name, parent_store, child_store)
                    break  # Exit the loop after successfully processing the files in a directory

                except error_perm as e:
                    logger.info(f"Folder not found: {directory}. Error: {e}")
                    continue  # Try the next directory if this one does not exist
                except Exception as e:
                    logger.error(f"Unexpected error during file listing: {e}")
                    raise

            # Send an alarm if daily data is missing
            if 'daily' in date_path and not daily_data_found:
                subject = f"SIDEKICK: Missing daily folder on FTP for the dealer {parent_store}-{child_store}."
                message = f"No daily data found for the dealer: {parent_store}-{child_store}."
                self.alert_topic(subject, message)
                logger.error(message)

        except (error_perm, error_temp) as e:
            logger.error(f"FTP transfer error: {e}")
            raise ConnectionError(f"FTP transfer error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during FTP to S3 transfer: {e}")
            raise
        finally:
            # Ensure the FTP connection is closed properly
            if self.connected_ftp:
                self.connected_ftp.quit()
