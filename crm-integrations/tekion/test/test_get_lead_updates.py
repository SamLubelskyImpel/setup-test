from app.get_lead_updates import lambda_handler
import boto3
from json import dumps, loads


def test_lead_update(requests_mock, mock_aws_resources):
    # Test setup
    secretsmanager = boto3.client('secretsmanager', region_name='us-east-1')
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')

    s3.create_bucket(Bucket='test-bucket')
    sqs.create_queue(QueueName='test-queue')
    secretsmanager.create_secret(
        Name='test/crm-integrations-partner',
        SecretString=dumps({'TEKION_V3': '{"url": "https://test.com", "app_id": "client_id", "access_key": "access_key"}'})
    )
    s3.put_object(Bucket='test-bucket', Key='configurations/test_TEKION.json', Body=dumps({
        'lead_updates_queue_url': 'https://sqs.us-east-1.amazonaws.com/123456789012/test-queue'
    }))
    s3.put_object(Bucket='test-bucket', Key='tekion_crm/token.json', Body=dumps({
        'token': 'api_token'
    }))

    mocked_tekion_response = {
        'data': [{
            'assignees': [{
                'arcId': 'assignee-1',
                'firstName': 'John',
                'lastName': 'Doe',
                'type': 'SALES_PERSON'
            }],
            'status': 'New'
        }]
    }
    requests_stub = requests_mock.get('https://test.com/openapi/v3.1.0/crm-leads?id=101112', json=mocked_tekion_response)

    # Run handler
    handler_res = lambda_handler({
        "crm_dealer_id": "123",
        "dealer_integration_partner_id": "456",
        "lead_id": "789",
        "crm_lead_id": "101112"
    }, None)

    # Assert request to Tekion
    request_to_tekion = requests_stub.request_history[0]
    assert request_to_tekion.headers['app_id'] == 'client_id'
    assert request_to_tekion.headers['dealer_id'] == '123'
    assert request_to_tekion.headers['Authorization'] == 'Bearer api_token'

    # Assert raw response saved to S3
    raw_response_key = [obj['Key'] for obj in s3.list_objects_v2(Bucket='test-bucket')['Contents'] if obj['Key'].startswith('raw_updates/tekion/')][0]
    raw_response = loads(s3.get_object(Bucket='test-bucket', Key=raw_response_key)['Body'].read().decode('utf-8'))
    assert raw_response == mocked_tekion_response

    # Assert lead published to SQS
    messages = sqs.receive_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/123456789012/test-queue')['Messages']
    assert loads(messages[0]['Body']) == {
        'lead_id': '789',
        'dealer_integration_partner_id': '456',
        'status': 'New',
        'salespersons': [{
            'crm_salesperson_id': 'assignee-1',
            'first_name': 'John',
            'last_name': 'Doe',
            'position_name': 'SALES_PERSON'
        }]
    }

    # Assert handler response
    assert handler_res == {
        'statusCode': 200,
        'body': dumps({
            'status': 'New',
            'salespersons': [{
                'crm_salesperson_id': 'assignee-1',
                'first_name': 'John',
                'last_name': 'Doe',
                'position_name': 'SALES_PERSON'
            }]
        })
    }