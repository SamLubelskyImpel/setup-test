import boto3
import os

# Constants
AWS_PROFILE = os.environ.get("AWS_PROFILE", "unified-test")
BUCKET_REGION = "prod" if AWS_PROFILE == "unified-prod" else "test"
BUCKET_NAME = f"integrations-us-east-1-{BUCKET_REGION}"

# Initialize clients
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def list_files_in_bucket(bucket_name, prefix):
    """List all the files in a given bucket with a given prefix."""
    files = []
    continuation_token = None
    while True:
        try:
            if continuation_token:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=continuation_token
                )
            else:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                )
        except Exception as e:
            print(f"Error occurred while listing objects: {e}")
            break

        if "Contents" in response:
            files.extend(obj["Key"] for obj in response["Contents"])

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    return files



def get_queue_url(queue_name):
    """Get the queue URL for the given queue name."""
    response = sqs_client.get_queue_url(QueueName=queue_name)
    return response["QueueUrl"]


def reprocess_files(queue_name, prefix):
    """Reprocess files in the given bucket prefix."""
    # Get queue URL
    queue_url = get_queue_url(queue_name)
    # List files in the bucket prefix
    files = list_files_in_bucket(BUCKET_NAME, prefix)
    # Confirmation
    confirmation = input(
        f"Reprocessing {len(files)} files in {prefix} to {queue_name}, are you sure? (y/n): "
    )
    if confirmation.lower() == "y":
        # Send each file as a message to SQS
        for i, file in enumerate(files, start=1):
            message_body = {
                "Records": [
                    {
                        "s3": {
                            "object": {"key": file},
                            "bucket": {"name": BUCKET_NAME}
                        }
                    }
                ]
            }
            sqs_client.send_message(
                QueueUrl=queue_url, MessageBody=str(message_body)
            )
            print(
                f"Sent {i} of {len(files)} with message {message_body} to queue {queue_url}"
            )
    else:
        print("Ignoring files")


if __name__ == "__main__":
    queue_name = "SidekickFormatROQueue"
    prefix = "sidekick/repair_order/"
    reprocess_files(queue_name, prefix)
