from datetime import datetime, date
import boto3
import os


BUCKET = os.environ.get('BUCKET')
FAILURES_QUEUE = os.environ.get('FAILURES_QUEUE')


def parsed_date(date_format: str, raw_date: str) -> date:
        try:
            return datetime.strptime(raw_date, date_format)
        except Exception:
            return None

def parsed_int(v) -> int:
    if isinstance(v, str):
        v = v.replace(',', '')
    int(v) if v else None

def save_progress(id: str, key: str):
    S3 = boto3.client('s3')
    S3.put_object(
        Body=str(id),
        Bucket=BUCKET,
        Key=key
    )

def load_progress(key: str):
    S3 = boto3.client('s3')
    try:
        return S3.get_object(
            Bucket=BUCKET,
            Key=key
        )['Body'].read().decode('utf-8')
    except Exception:
        return 0

def publish_failure(record: dict, err: str, table: str):
        SQS = boto3.client('sqs')
        SQS.send_message(
            QueueUrl=FAILURES_QUEUE,
            MessageAttributes={
                'Table': {
                    'DataType': 'String',
                    'StringValue': table
                },
                'RecordId': {
                    'DataType': 'String',
                    'StringValue': str(record['id'])
                }
            },
            MessageBody=err
        )