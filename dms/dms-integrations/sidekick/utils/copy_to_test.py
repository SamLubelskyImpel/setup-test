""" Copy the prod s3 data to test. """
from os import remove

import boto3


def download_file_from_s3(bucket_name, object_key, local_filename, is_prod=False):
    env_name = "prod" if is_prod else "test"
    profile_name = f"unified-{env_name}"
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client("s3")
    s3_client.download_file(bucket_name, object_key, local_filename)


def upload_file_to_s3(bucket_name, object_key, local_filename, is_prod=False):
    env_name = "prod" if is_prod else "test"
    profile_name = f"unified-{env_name}"
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client("s3")
    with open(local_filename, "rb") as f:
        s3_client.put_object(Body=f, Bucket=bucket_name, Key=object_key)


def delete_file_from_s3(bucket_name, object_key, is_prod=False):
    env_name = "prod" if is_prod else "test"
    profile_name = f"unified-{env_name}"
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client("s3")
    s3_client.delete_object(Bucket=bucket_name, Key=object_key)


def list_files_in_bucket(bucket_name, prefix, is_prod=False):
    env_name = "prod" if is_prod else "test"
    profile_name = f"unified-{env_name}"
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client("s3")

    files = []

    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if "Contents" in response:
            for obj in response["Contents"]:
                files.append(obj["Key"])

        if "NextContinuationToken" in response:
            continuation_token = response["NextContinuationToken"]
        else:
            # No more continuation tokens, exit the loop
            break

    return files


for integration in ["repair_order"]:
    prefix = f"sidekick/{integration}/"
    prod_bucket_name = "integrations-us-east-1-prod"
    prod_file_paths = list_files_in_bucket(prod_bucket_name, prefix, is_prod=True)
    test_bucket_name = "integrations-us-east-1-test"
    test_file_paths = list_files_in_bucket(test_bucket_name, prefix, is_prod=False)
    for prod_file_path in prod_file_paths:
        if prod_file_path in test_file_paths:
            print(f"Skipping {prod_file_path}")
        else:
            temp_file_name = "sidekick_temp"
            download_file_from_s3(
                prod_bucket_name, prod_file_path, temp_file_name, is_prod=True
            )
            upload_file_to_s3(
                test_bucket_name, prod_file_path, temp_file_name, is_prod=False
            )
            remove(temp_file_name)
            print(f"Added {prod_file_path}")
