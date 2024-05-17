import boto3
import logging
from os import environ
from json import loads
from datetime import date, datetime, timezone, timedelta
from ftp_wrapper import FtpToS3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = f"integrations-us-east-1-{'prod' if ENVIRONMENT == 'prod' else 'test'}"
SNS_TOPIC_ARN = environ.get("CE_TOPIC")
SNS_CLIENT = boto3.client('sns')

def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/TekionFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise


def list_recent_files_ftp(ftp_server, ftp_user, ftp_password, ftp_directory):
    """List files modified in the last 24 hours from the FTP server."""
    ftp = ftplib.FTP(ftp_server)
    ftp.login(user=ftp_user, passwd=ftp_password)
    ftp.cwd(ftp_directory)
    
    recent_files = []
    current_time = datetime.now()
    time_limit = current_time - timedelta(days=1)
    
    for file_name in ftp.nlst():
        try:
            # Get the modification time of the file
            modified_time_str = ftp.sendcmd(f"MDTM {file_name}")[4:].strip()
            modified_time = datetime.strptime(modified_time_str, "%Y%m%d%H%M%S")
            
            # Check if the file was modified in the last 24 hours
            if modified_time > time_limit:
                recent_files.append(file_name)
        except Exception as e:
            print(f"Error retrieving modification time for {file_name}: {e}")
    
    ftp.quit()
    return recent_files


def lambda_handler(event, context):
    secrets = get_ftp_credentials()
    ftp_session = FtpToS3(**secrets)
    
    # List recent files from the FTP server
    recent_files = list_recent_files_ftp(ftp_server, ftp_user, ftp_password, ftp_directory)
    
    for file_name in recent_files:
        # Get the file from the FTP server
        file_obj = get_file_from_ftp(ftp_server, ftp_user, ftp_password, ftp_directory, file_name)
        
        # Define the S3 key
        s3_key = f"{s3_key_prefix}/{file_name}"
        
        # Upload the file to S3
        upload_file_to_s3(file_obj, s3_bucket_name, s3_key)
        
        print(f"File {file_name} from FTP has been uploaded to s3://{s3_bucket_name}/{s3_key}")
    
    return {
        'statusCode': 200,
        'body': f"Processed {len(recent_files)} files from FTP to S3."
    }
