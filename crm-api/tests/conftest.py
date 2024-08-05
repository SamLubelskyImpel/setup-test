import pytest
import json
import boto3
from uuid import uuid4


unified_session = boto3.session.Session(profile_name='unified-test')


@pytest.fixture()
def lead_data():
    data = json.load(open('tests/lead_example.json'))
    data['crm_lead_id'] = str(uuid4())
    return data


@pytest.fixture()
def activity_data():
    return json.load(open('tests/activity_example.json'))


@pytest.fixture()
def auth():
    secret = unified_session.client('secretsmanager').get_secret_value(SecretId='test/crm-api')
    secret = json.loads(secret['SecretString'])['test']
    secret_data = json.loads(secret)
    return {
        'partner_id': 'test',
        'x_api_key': secret_data['api_key']
    }
