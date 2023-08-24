import os
import boto3

os.environ["AWS_PROFILE"] = "unified-prod"


S3_CLIENT = boto3.client("s3")
bucket_name = "kawasaki-us-east-1-prod"
file_key = "kawasaki_config.json"
current_directory = os.getcwd()
parent_directory = os.path.dirname(os.path.abspath(current_directory))
local_file_path = f"{parent_directory}/{file_key}"
S3_CLIENT.download_file(bucket_name, file_key, local_file_path)
print(f"Downloaded: {local_file_path}")
