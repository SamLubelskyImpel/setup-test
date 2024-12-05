import random
from uuid import uuid4
from datetime import datetime
import boto3

import os

os.environ["AWS_PROFILE"] = 'unified-test'

HEADERS = "EXT_CONSUMER_ID|^|DEALER_IDENTIFIER|^|PII|^|PII_Type|^|FDGUID|^|FDDGUID|^|STATUS|^|pacode|^|MATCH_RESULT"


def upload_match_file(rows, pacode: str, filename: str):
    current_date = datetime.now()
    s3_key = f"fd-raw/pii_match/{pacode}/{current_date.year}/{current_date.month}/{current_date.day}/{filename}"

    with open(filename, 'w') as file:
        file.write(HEADERS + "\n")
        file.writelines(rows)

    s3_client = boto3.client('s3')

    with open(filename, 'rb') as file:
        # Upload the in-memory CSV to S3
        s3_client.put_object(Bucket='cdpi-shared-us-east-1-test', Key=s3_key, Body=file)

# Consumer IDs pulled from test.cdpi_consumer table, those created by trigger_consumer_ingestion, or manually
"""
Example csv file:
123
1233
"""
with open('cdpi_consumer_ids.csv', 'r') as f:
    CONSUMERS = f.read().splitlines()
    f.close()

entries = []
pa_code = "00000"
cdpi_dealer_id = "2"

pii_options = [
    ("phone", "00000"),
    ("email", "testemail.con"),
    ("fName", "test"),
    ("lName", "test")
]

for consumer in CONSUMERS:
    pii, pii_value = random.choice(pii_options)
    cdp_master_consumer_id = str(uuid4())
    cdp_dealer_consumer_id = str(uuid4())

    entries.append("{}|^|{}|^|{}|^|{}|^|{}|^|{}|^|{}|^|{}|^|{}\n".format(
        consumer,
        cdpi_dealer_id,
        pii_value,
        pii,
        cdp_master_consumer_id,
        cdp_dealer_consumer_id,
        "SUCCESS",
        pa_code,
        "1"
    ))


timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
output_file = f"eid_pii_match_test_impel_{cdpi_dealer_id}_{timestamp}.txt"

print(output_file)
# upload file to S3 cdpi-shared bucket fd-raw/consumer_profile_summary/11111 (or any other dealer_id)
upload_match_file(entries, "00000", output_file)
