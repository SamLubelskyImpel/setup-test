"""Download a historical file from FTP, split it into smaller chunks and upload to S3.
This script takes a filename as a command line argument, downloads the file from FTP,
splits the file into smaller chunks (30 MB), then uploads each chunk to S3 for processing.

Notes to consider:
    - This script is only handles RO, F&I, and SA historical files.
    - Historical files vary in naming convention. Make sure to update this script is setup to identify the file type.
      (Pending standardization from Tekion DMS)

AWS_PROFILE=unified-admin python ./ftp_to_s3_csv_splitter.py 2201580_RO.csv
"""

import boto3
from ftplib import FTP
import argparse
import os
import csv
import json
from io import StringIO
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
    Split a large CSV file into smaller chunks of size 'max_size' (default: 60MB).
    Yields each chunk one by one.
    """
    max_size = 30 * 1024 * 1024  # 30 MB
    header = None

    # Determine if the file is gzipped based on its extension
    open_func = gzip.open if filename.endswith('.gz') else open

    with open_func(filename, 'rt', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        header.append("dms_id")  # Add the dms_id column to the header

        # Start a buffer for the first chunk
        chunk_buffer = StringIO()
        writer = csv.writer(chunk_buffer)
        writer.writerow(header)  # Write the header to the buffer once per chunk
        current_size = chunk_buffer.tell()  # Initialize size with header size

        for row in reader:
            # Append the dms_id to each row
            row.append(dms_id)

            # Write the row to the chunk buffer
            row_buffer = StringIO()
            row_writer = csv.writer(row_buffer)
            row_writer.writerow(row)
            row_data = row_buffer.getvalue()
            row_size = len(row_data.encode('utf-8'))

            # If adding this row exceeds max file size, yield the chunk and start a new one
            if current_size + row_size > max_size:
                yield chunk_buffer.getvalue()  # Yield the content of the current chunk

                # Start a new chunk with the header
                chunk_buffer = StringIO()
                writer = csv.writer(chunk_buffer)
                writer.writerow(header)
                current_size = chunk_buffer.tell()  # Reset the size counter with header size

            # Add the row to the current chunk and update size
            chunk_buffer.write(row_data)
            current_size += row_size

        # Yield any remaining rows in the last chunk
        if current_size > 0:
            yield chunk_buffer.getvalue()


def upload_to_s3(data, bucket_name, object_name):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=data)
    except Exception as e:
        print(f"Error uploading to S3: {e}")


def get_ftp_credentials():
    """Get FTP credentials from secretsmanager."""
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId=f"{'prod' if AWS_PROFILE in ('unified-prod','unified-admin') else 'test'}/TekionFTP"
    )
    secret = json.loads(secret["SecretString"])
    return secret


def main():
    parser = argparse.ArgumentParser(description='Download file from FTP, split and upload to S3.')
    parser.add_argument('filename', type=str, help='Filename on the FTP server to be downloaded.')
    args = parser.parse_args()
    filename = args.filename
    dms_id = filename.split("_")[0]

    filetype = ''

    if "RO" in filename:
        filetype = 'repair_order'
    elif "VS" in filename:
        filetype = 'fi_closed_deal'
    elif "SA" in filename:
        filetype = 'service_appointment'
    else:
        print('Please, include a file type in the filename: RepairOrders or Deals')

    ftp_credentials = get_ftp_credentials()
    host = ftp_credentials['host']
    user = ftp_credentials['user']
    password = ftp_credentials['password']
    remote_directory = f"/{'prod' if AWS_PROFILE in ('unified-prod','unified-admin') else 'test'}_tekion_dms"  # Directory on the FTP server where the file is located
    local_file_path = os.path.join(os.getcwd(), filename)  # Save the file in the current directory

    ftp = connect_to_ftp(host, user, password)

    if ftp:
        download_from_ftp(ftp, remote_directory, args.filename, local_file_path)
        # Split and upload to S3
        now = datetime.now()
        year, month, day = now.year, now.month, now.day

        bucket_name = f"integrations-us-east-1-{'prod' if AWS_PROFILE in ('unified-prod','unified-admin') else 'test'}"
        print(f"Uploading to bucket: {bucket_name}")
        for part_number, data in enumerate(split_file(local_file_path, dms_id), 1):
            print(part_number)
            object_name = f'tekion-apc/historical/{filetype}/{dms_id}/{year}/{month}/{day}/{dms_id}_historical_{part_number}.json'
            upload_to_s3(data, bucket_name, object_name)


if __name__ == '__main__':
    main()
