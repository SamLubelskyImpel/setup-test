from moto import mock_aws
import boto3
import json
from app.handler import lambda_handler
import pytest
        
@pytest.fixture
def mock_ses(mocker):
    yield mocker.patch('app.handler.SES.send_email')

@mock_aws        
def test_handler(mock_ses):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    s3.put_object(Bucket='test-bucket', Key='test-key', Body=json.dumps({
        'recipients': ['to1@email.com', 'to2@email.com'],
        'subject': 'Test Subject',
        'body': 'Test Body'
    }))
    
    lambda_handler({ 'Records': [{
        's3': {
            'bucket': {'name': 'test-bucket'},
            'object': {'key': 'test-key'}
        }
    }]}, None)
    
    assert mock_ses.called_once_with(
        Source='test-source',
        Destination={'ToAddresses': ['to1@email.com', 'to2@email.com']},
        Message={'Subject': {'Data': 'Test Subject'}, 'Body': {'Text': {'Data': 'Test Body'}}}
    )