import pytest
from moto import mock_aws
import os

os.environ["SECRET_KEY"] = "TEKION_V3"
os.environ["INTEGRATIONS_BUCKET"] = "test-bucket"
os.environ["ENVIRONMENT"] = "test"

@pytest.fixture()
def mock_aws_resources():
    with mock_aws() as mock:
        yield mock

@pytest.fixture()
def mock_requests_get(mocker):
    mock = mocker.patch('requests.get')
    yield mock