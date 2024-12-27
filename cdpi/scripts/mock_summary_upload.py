import random
from datetime import datetime
import csv
import boto3

import os

os.environ["AWS_PROFILE"] = 'unified-test'

# File headers
HEADERS = "c_fdguid|^|signal_count|^|signal_first|^|signal_latest|^|dealer_distinct_count|^|vin_distinct_count|^|vin_bought_distinct_count|^|vin_serviced_distinct_count|^|custom_purchaseorlease_first|^|custom_purchaseorlease_latest|^|actor_consumer_count|^|actor_consumer_first|^|actor_consumer_latest|^|epm_sales_bucket|^|epm_sales_decile|^|esm_service_bucket|^|esm_service_decile|^|voi|^|voi_date|^|web_voi|^|web_voi_date|^|actor_consumer_latest_signal_category|^|actor_consumer_latest_signal_topic|^|actor_consumer_latest_signal_detail|^|in_market_sales_status|^|in_market_sales_consumer|^|current_owner|^|current_owner_signal_date|^|communication_preference|^|communication_preference_signal_date|^|EXT_CONSUMER_ID|^|operation"
row_schema = "{}|^|23|^|2019-04-26|^|2024-05-17|^|3|^|1|^|1|^|0|^|2024-05-17|^|2024-05-17|^|9|^|2019-04-26|^|2024-05-17|^|Medium|^|{}|^|Medium|^|{}|^||^||^||^||^|Transaction|^|Vehicle|^|Purchase|^|Dead|^|0|^||^||^||^||^|{}|^|A\n"


def upload_summary_file(rows, pacode: str, filename: str):
    current_date = datetime.now()
    s3_key = f"fd-raw/consumer_profile_summary/{pacode}/{current_date.year}/{current_date.month}/{current_date.day}/{filename}"

    with open(filename, 'w') as file:
        file.write(HEADERS + "\n")
        file.writelines(rows)

    s3_client = boto3.client('s3')

    with open(filename, 'rb') as file:
        # Upload the in-memory CSV to S3
        s3_client.put_object(Bucket='cdpi-shared-us-east-1-test', Key=s3_key, Body=file)


"""Requied input is a CSV file containing consumer_id, cdp_dealer_consumer_id from cdpi_consumer_profile table
Select newly matched consumers from mock_matched_upload.py, "SELECT consumer_id, cdp_dealer_consumer_id FROM cdpi_consumer_profile ..."
Export from database as csv

Example csv file:
consumer_id,cdp_dealer_consumer_id
123,123
"""
input_file = "cdpi_consumer_profile_202411211146.csv"
with open(input_file, 'r') as f:
    rows = [row for row in csv.DictReader(f)]
    f.close()

# Create dummy summary file for matched consumer profiles
entries = []
for consumer in rows:
    pscore_sales = int(str(random.uniform(0, 11)).split('.')[0])
    pscore_service = int(str(random.uniform(0, 11)).split('.')[0])

    entry = row_schema.format(
        consumer['cdp_dealer_consumer_id'],
        pscore_sales,
        pscore_service,
        consumer['consumer_id']
    )
    entries.append(entry)

timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
output_file = f"consumerprofilesummary_impel_00000_{timestamp}.txt"

# upload file to S3 cdpi-shared bucket fd-raw/consumer_profile_summary/11111 (or any other dealer_id)
upload_summary_file(entries, "00000", output_file)
