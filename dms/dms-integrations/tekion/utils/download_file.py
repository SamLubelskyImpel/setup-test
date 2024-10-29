"""Get historical data from FTP."""
import boto3
from json import loads
from datetime import datetime
from ftplib import FTP, error_perm, error_temp
import os

if os.environ.get("AWS_PROFILE") in ('unified-prod', 'unified-admin'):
    FTP_FOLDER = "prod_tekion_dms"
    ENVIRONMENT = "prod"
    INTEGRATIONS_BUCKET = "integrations-us-east-1-prod"
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:196800776222:alert_client_engineering"
else:
    FTP_FOLDER = "test_tekion_dms"
    ENVIRONMENT = "test"
    INTEGRATIONS_BUCKET = "integrations-us-east-1-test"
    SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:143813444726:alert_client_engineering"

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
            print("Error connecting to FTP: %s", e)
            raise


def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/TekionFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        secret_data = loads(secret["SecretString"])
        host = secret_data.get("host")
        user = secret_data.get("user")
        password = secret_data.get("password")
        return host, user, password
    except boto3.exceptions.Boto3Error as e:
        print(f"Failed to retrieve secret {secret_id}: {e}")
        raise


def upload_file_to_s3(local_file, s3_key):
    try:
        s3_client = boto3.client('s3')
        with open(local_file, 'rb') as data:
            s3_client.put_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=data)
            print(f"File {local_file} uploaded to S3 as {s3_key}.")
    except ValueError as e:
        print(f"Error uploading file {local_file} to S3: {e}")
        raise


def process_file(file, ftp, dealer_id, s3_date_path):
    try:
        if file.startswith(f"{dealer_id}_"):
            local_file = f"tmp/{file}"

            if any(keyword in file for keyword in ["RepairOrder", "RO", "SERVICE", "ServiceHistory"]):
                s3_key = f"tekion/historical/repair_order/{dealer_id}/{s3_date_path}/{file}"
            elif any(keyword in file for keyword in ["VehicleSales", "VSH", "SALES", "VS", "VehicleSaleHistory"]):
                s3_key = f"tekion/historical/fi_closed_deal/{dealer_id}/{s3_date_path}/{file}"
            else:
                raise ValueError(f"Unknown file type for file {file}")

            upload_file_to_s3(local_file, s3_key)
    except Exception as e:
        print(f"Error processing file {file}: {e}")
        raise


def parse_data(dealer_id, end_dt, new_files):
    """Parse and handle SQS Message."""
    try:
        s3_date_path = datetime.strptime(end_dt, "%Y-%m-%dT%H:%M:%S").strftime("%Y/%m/%d")
        host, user, password = get_ftp_credentials()
        ftp_session = FtpToS3(host=host, user=user, password=password)
        ftp = ftp_session.connect_to_ftp()

        if ftp:
            print(f"New files found in the last 24 hours: {new_files}")
            if new_files:
                for file in new_files:
                    try:
                        process_file(file, ftp, dealer_id, s3_date_path)
                    except Exception as e:
                        print(f"Error processing file {file}: {e}")
                        raise
            else:
                print(f"No new files found in the last 24 hours for dealer {dealer_id}.")
        else:
            print(f"Dealer {dealer_id} folder not found in FTP server.")
    except Exception as e:
        print(f"Error parsing data: {e}")
        raise


if __name__ == '__main__':
    dealer_id = "7592678"
    end_dt_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    filenames = ["7592678_VS.csv", "7592678_RO.csv"]

    parse_data(dealer_id, end_dt_str, filenames)
