import boto3
import json
from datetime import datetime
from uuid import uuid4
from mock_carsales_vehicle_event import get_event

def upload_json_to_s3(event, s3_key):
    bucket_name = 'inventory-integrations-us-east-1-matheus-nearsure-ce-1850'

    try:
        # Create an S3 client using the profile from the AWS_PROFILE environment variable
        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json.dumps(event))
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    for _ in range(15):
        now = datetime.now()
        dealer, vin, event = get_event()
        s3_file_key = f'raw/carsales/{dealer}/{now.year}/{now.month}/{now.day}/{now.isoformat()}_{str(uuid4())}.json'
        print(f'Dealer: {dealer}, VIN: {vin}, S3: {s3_file_key}')
        upload_json_to_s3(event, s3_file_key)
