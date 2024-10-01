import pytest
import json
from uuid import uuid4


@pytest.fixture()
def event():
    data = json.load(open('tests/sample_event.json'))
    data['events'][0]['event_id'] = str(uuid4())
    return data


@pytest.fixture()
def writeback_event():
    data = json.load(open('tests/sample_writeback_event.json'))
    data['events'][0]['event_id'] = str(uuid4())
    return data
