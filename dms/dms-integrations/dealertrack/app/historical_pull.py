import logging
import os
import sys
from rds_instance import RDSInstance
from data_pull_manager import DataPullManager
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from json import dumps, loads
from time import sleep
from uuid import uuid4


IS_PROD = os.environ.get("ENVIRONMENT") == "prod"
STOP_AT = int(os.environ.get("STOP_AT", 13))
MAX_DAYS_PULL = int(os.environ.get("MAX_DAYS_PULL", 1))
QUEUE_URL = os.environ.get("QUEUE_URL")


logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),  # Send logs to stdout
    ],
)
logger = logging.getLogger()
rds = RDSInstance(is_prod=IS_PROD)
sqs = boto3.client("sqs")


def get_current_time():
    return datetime.now(tz=timezone.utc)


def str_to_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d")


def get_next_pull_date(last_processed_date: str = None):
    return (
        (
            datetime.strptime(last_processed_date, "%Y-%m-%d")
            if last_processed_date
            else get_current_time()
        )
        - timedelta(days=1)
    ).strftime("%Y-%m-%d")


class HistoricalPullManager:

    def __init__(self, dms_id: str, pull_date: str, started_at: str):
        self.dms_id = dms_id
        self.pull_date = pull_date
        self.started_at = started_at

    def start(self):
        logger.info(f"Starting data pull {self.dms_id} {self.pull_date}")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    DataPullManager(self.dms_id, self.pull_date, "fi_closed_deal").start
                ),
                executor.submit(
                    DataPullManager(self.dms_id, self.pull_date, "repair_order").start
                ),
            ]
            for r in as_completed(futures):
                r.result()

        if str_to_date(self.pull_date) <= str_to_date(self.started_at) - timedelta(
            days=MAX_DAYS_PULL
        ):
            dealer_metadata = {
                "status": "FINISHED",
            }
            rds.save_historical_progress([self.dms_id], dealer_metadata)


def dispatch_events_for_dealer(dms_id: str):
    msgs = []
    for i in range(1, MAX_DAYS_PULL + 1):
        msgs.append(
            {
                "dms_id": dms_id,
                "pull_date": (get_current_time() - timedelta(days=i)).strftime(
                    "%Y-%m-%d"
                ),
                "started_at": get_current_time().strftime("%Y-%m-%d"),
            }
        )
    for i in range(0, len(msgs), 10):
        batch = msgs[i:i + 10]
        sqs.send_message_batch(
            QueueUrl=QUEUE_URL,
            Entries=[{"Id": str(uuid4()), "MessageBody": dumps(msg)} for msg in batch],
        )


def support_alert(content):
    logger.error(f"[SUPPORT ALERT] Error on historical pull [CONTENT] {content}")


def onboard_dealers():
    try:
        active_dealers = rds.select_db_historical_active_dealer_partners(
            "dealertrack-dms"
        )
        if active_dealers:
            logger.info(f"Found {len(active_dealers)} new dealers {active_dealers}")
            for dms_id in active_dealers:
                dispatch_events_for_dealer(dms_id)
            rds.save_historical_progress(active_dealers, {"status": "IN_PROGRESS"})
            wait_visibility_timeout()
    except Exception as e:
        msg = f"Failed to dispatch events for onboarded dealers: {e}"
        logger.exception(msg)
        support_alert(msg)


def historical_pull_for_dealer(dms_id, events):
    logger.info(f"Processing events for {dms_id}: {events}")
    for event in events:
        receipt_handle = event["ReceiptHandle"]
        try:
            HistoricalPullManager(
                dms_id, event["pull_date"], event["started_at"]
            ).start()
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
        except Exception:
            logger.exception(f"Failed to process event {event}")


def run_pull_concurrently(events_by_dealer):
    if not events_by_dealer:
        return

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(historical_pull_for_dealer, dms_id, events)
            for dms_id, events in events_by_dealer.items()
        ]

    for r in as_completed(futures):
        r.result()


def group_events_by_dealer(raw_events):
    events_by_dealer = {}

    for event in raw_events:
        body = loads(event["Body"])
        dms_id = body["dms_id"]
        body["ReceiptHandle"] = event["ReceiptHandle"]

        if dms_id not in events_by_dealer:
            events_by_dealer[dms_id] = []

        events_by_dealer[dms_id] += [body]

    return events_by_dealer


def get_batch_of_events():
    events = []
    for _ in range(3):
        events += sqs.receive_message(
            QueueUrl=QUEUE_URL, WaitTimeSeconds=20, MaxNumberOfMessages=10
        ).get("Messages", [])
    return events


def wait_visibility_timeout():
    sleep(60)


if __name__ == "__main__":
    try:
        logger.info(f"Task started at {get_current_time()}")

        # dispatch events for recently onboarded dealers
        onboard_dealers()

        empty_attempts = 0

        while get_current_time().hour < STOP_AT:
            events = get_batch_of_events()

            if not events and empty_attempts > 5:
                logger.info("No historical events to process")
                break   # stop the task to save costs

            if not events:
                empty_attempts += 1
                wait_visibility_timeout()
                continue

            logger.info(f"Received events: {events}")
            events_by_dealer = group_events_by_dealer(events)
            run_pull_concurrently(events_by_dealer)
    except Exception as e:
        support_alert(str(e))
        logger.exception("Error on historical pull")
    finally:
        logger.info(f"Task stopped at {get_current_time()}")
