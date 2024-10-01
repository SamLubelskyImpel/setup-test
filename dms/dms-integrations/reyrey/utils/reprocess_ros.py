""" Reprocess ReyRey RO files. """
from json import dumps
from os import environ

import boto3

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
AWS_PROFILE = environ.get("AWS_PROFILE", "unified-test")


def list_files_in_bucket(bucket_name, prefix, files=[], continuation_token=None):
    """List all the files in a given bucket with a given prefix."""
    if continuation_token:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token
        )
    else:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" in response:
        for obj in response["Contents"]:
            files.append(obj["Key"])

    if not response.get("IsTruncated"):
        return files

    continuation_token = response.get("NextContinuationToken")
    return list_files_in_bucket(bucket_name, prefix, files, continuation_token)


def get_queue_url(queue_name):
    """Get the queue url for the given queue name."""
    response = sqs_client.get_queue_url(QueueName=queue_name)
    return response["QueueUrl"]


def reprocess_files(queue_name, prefix):
    bucket_name = (
        f"integrations-us-east-1-{'prod' if AWS_PROFILE == 'unified-prod' else 'test'}"
    )
    queue_url = get_queue_url(queue_name)
    files = list_files_in_bucket(bucket_name, prefix, files=[], continuation_token=None)
    confirmation = input(
        f"Reprocessing {len(files)} files in {prefix} to {queue_name}, are you sure? (y/n): "
    )
    if confirmation == "y":
        for i, file in enumerate(files):
            records = [
                {"s3": {"object": {"key": file}, "bucket": {"name": bucket_name}}}
            ]
            sqs_message = dumps({"Records": records})
            sqs_client.send_message(QueueUrl=queue_url, MessageBody=sqs_message)
            print(
                f"Sent {i + 1} of {len(files)} with message {sqs_message} to queue {queue_url}"
            )
    else:
        print("Ignoring files")


queue_name = "ReyReyFormatROQueue"
prefix = "reyrey/repair_order/"
reprocess_files(queue_name, prefix)
