'''
Run this test to verify that the email service is working as expected.
AWS_PROFILE=unified-test ENV={YOUR_ENV} EMAIL={YOUR_VERIFIED_EMAIL} pytest

Note that you need to deploy the email service before running this test.
YOUR_ENV and YOUR_VERIFIED_EMAIL should be the same as the ones you used in the deployment.
'''

import os
import boto3
import json
from uuid import uuid4
from time import sleep

ENV = os.environ.get('ENV')
EMAIL = os.environ.get('EMAIL')
S3 = boto3.client('s3', region_name='us-east-1')
LOGS = boto3.client('logs')
SES = boto3.client('ses')
BUCKET = f'email-service-store-{ENV}'
LOG_GROUP = f'/aws/lambda/email-service-delivery-monitoring-{ENV}'

def test_successful_delivery():
    email_id = str(uuid4())
    
    S3.put_object(Bucket=BUCKET, Key='automated-test.json', Body=json.dumps({
        "recipients": [EMAIL],
        "subject": email_id,
        "body": "This is a test email",
        "from_address": EMAIL,
        "reply_to": ["fake@email.com"]
    }))
    
    attempts = 0
    send_event, delivery_event = None, None
    while attempts < 6:
        sleep(20)
        logs = [log for log in get_recent_log_events() if f'"subject":"{email_id}"' in log]
        send_event = [event for event in logs if '"eventType":"Send"' in event]
        delivery_event = [event for event in logs if '"eventType":"Delivery"' in event]
        if send_event and delivery_event:
            break
        attempts += 1
        
    assert send_event, 'No send event found'
    assert delivery_event, 'No delivery event found'
    
def get_recent_log_events():
    client = boto3.client('logs')
    
    response = client.describe_log_streams(
        logGroupName=LOG_GROUP,
        orderBy='LastEventTime',
        descending=True,
        limit=1
    )
    
    log_streams = response['logStreams']
    if not log_streams:
        raise ValueError("No log streams found")
    
    response = client.get_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=log_streams[0]['logStreamName'],
        limit=20  # Adjust as needed
    )
    
    return [event['message'] for event in response['events']]
