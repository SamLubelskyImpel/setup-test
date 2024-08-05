from time import sleep
from utils import get_wbns_log_events, send_event


def test_monitoring_lambda(event):
    send_event(event)

    event_id = event['events'][0]['event_id']
    attempts = 0
    log = None

    while attempts < 6:
        sleep(5)
        log = next(iter([log for log in get_wbns_log_events() if f"'event_id': '{event_id}'" in log]), None)
        if log:
            break
        attempts += 1

    assert log is not None
