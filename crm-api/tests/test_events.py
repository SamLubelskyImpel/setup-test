import requests
from os import environ
from time import sleep
from utils import get_ins_log_events


BASE_URL = environ.get('BASE_URL')


def test_lead_created_event(lead_data, auth):
    res = requests.post(
        f'{BASE_URL}/leads',
        json=lead_data,
        headers={**auth})

    assert res.status_code == 201
    lead_id = res.json()['lead_id']

    attempts = 0
    while attempts < 6:
        sleep(5)
        log = next(iter([log for log in get_ins_log_events() if f"'lead_id': {lead_id}" in log]), None)
        if log:
            break
        attempts += 1

    assert log is not None


def test_activity_created_event(activity_data, auth):
    res = requests.post(
        f'{BASE_URL}/activities?lead_id=449179',
        json=activity_data,
        headers={**auth})

    assert res.status_code == 201
    activity_id = res.json()['activity_id']

    attempts = 0
    while attempts < 6:
        sleep(5)
        log = next(iter([log for log in get_ins_log_events() if f"'activity_id': {activity_id}" in log]), None)
        if log:
            break
        attempts += 1

    assert log is not None
