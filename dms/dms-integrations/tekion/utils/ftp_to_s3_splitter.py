import boto3
from ftplib import FTP
import argparse
import os
import json
import gzip
from datetime import datetime

AWS_PROFILE = os.environ["AWS_PROFILE"]


def connect_to_ftp(host, user, password):
    try:
        ftp = FTP(host)
        ftp.login(user, password)
        return ftp
    except Exception as e:
        print(f"Error connecting to FTP: {e}")
        return None


def download_from_ftp(ftp, remote_directory, filename, local_file_path):
    try:
        ftp.cwd(remote_directory)
        with open(local_file_path, 'wb') as local_file:
            ftp.retrbinary('RETR ' + filename, local_file.write)
    except Exception as e:
        print(f"Error downloading file {filename} from FTP: {e}")


def split_file(filename, dms_id):
    """
    Split a large JSON file into smaller chunks of size 'max_size' (default: 60MB).
    Yields each chunk one by one.
    """
    current_size = 0
    max_size = 60*1024*1024
    first_obj = True
    chunk = []

    with gzip.open(filename, 'rt', encoding='utf-8') as f:
        data = json.load(f)
        for obj in data:
            # Append the dms_id
            obj["dms_id"] = dms_id

            obj_str = json.dumps(obj)
            if not first_obj:
                obj_str = "," + obj_str
            obj_str += "\n"
            # If adding the object exceeds the max file size, yield the current chunk and start a new one
            if current_size + len(obj_str) + 2 > max_size:
                yield "[\n" + "".join(chunk) + "]\n"
                chunk = []
                current_size = 0
                first_obj = True
            chunk.append(obj_str)
            current_size += len(obj_str)
            first_obj = False
        yield "".join(chunk)


def upload_to_s3(data, bucket_name, object_name):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=data)
    except Exception as e:
        print(f"Error uploading to S3: {e}")


def get_ftp_credentials():
    """Get FTP credentials from secretsmanager."""
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId=f"{'prod' if AWS_PROFILE == 'unified-prod' else 'test'}/TekionFTP"
    )
    secret = json.loads(secret["SecretString"])
    return secret


def main():
    parser = argparse.ArgumentParser(description='Download file from FTP, split and upload to S3.')
    parser.add_argument('filename', type=str, help='Filename on the FTP server to be downloaded.')
    args = parser.parse_args()
    filename = args.filename
    dms_id = filename.split("|")[0]

    filetype = ''

    if "RepairOrders" in filename:
        filetype = 'repair_order'
    elif "Deals" in filename:
        filetype = 'fi_closed_deal'
    else:
        print('Please, include a file type in the filename: RepairOrders or Deals')
    
    ftp_credentials = get_ftp_credentials()
    host = ftp_credentials['host']
    user = ftp_credentials['user']
    password = ftp_credentials['password']
    remote_directory = f"/{'prod' if AWS_PROFILE == 'unified-prod' else 'test'}_tekion_dms"  # Directory on the FTP server where the file is located
    local_file_path = os.path.join(os.getcwd(), filename)  # Save the file in the current directory
    
    ftp = connect_to_ftp(host, user, password)

    if ftp:
        download_from_ftp(ftp, remote_directory, args.filename, local_file_path)
        # Split and upload to S3
        now = datetime.now()
        year, month, day = now.year, now.month, now.day

        bucket_name = f"integrations-us-east-1-{'prod' if AWS_PROFILE == 'unified-prod' else 'test'}"
        for part_number, data in enumerate(split_file(local_file_path, dms_id), 1):
            object_name = f'tekion/{filetype}/{year}/{month}/{day}/{dms_id}_historical_Part_{part_number}.json'
            upload_to_s3(data, bucket_name, object_name)

if __name__ == '__main__':
    main()