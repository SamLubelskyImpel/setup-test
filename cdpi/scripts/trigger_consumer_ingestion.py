"""
Triggers the consumer ingestion pipeline by putting product files on the CDPI shared bucket.
Usage: AWS_PROFILE=unified-test python trigger_consumer_ingestion.py
"""

from uuid import uuid4
import random
from faker import Faker
from datetime import datetime
import csv
import io
import boto3

import os

os.environ["AWS_PROFILE"] = 'unified-test'


PRODUCT_COLUMNS = [
    "dealer_id",
    "customer_id",
    "salesforce_id",
    "first_name",
    "last_name",
    "phone_number",
    "email_address",
    "email_optin_flag",
    "phone_optin_flag",
    "sms_optin_flag",
    "address_1",
    "address_2",
    "suite",
    "city",
    "areatype",
    "area",
    "country",
    "zip",
    "zipextra",
    "pobox",
    "record_date"
]


def generate_rows(dealer_id: str, salesforce_id: str, row_count=50):
    """Generate a CSV file with random data."""
    rows = []

    for _ in range(row_count):
        faker = Faker()
        rows.append([
            dealer_id,
            str(uuid4()),
            salesforce_id,
            faker.first_name()[:80],
            faker.last_name()[:80],
            faker.phone_number()[:15],
            faker.email()[:100],
            random.choice([0, 1]),
            random.choice([0, 1]),
            random.choice([0, 1]),
            faker.street_address()[:80],
            faker.secondary_address()[:80],
            faker.building_number()[:80],
            faker.city()[:80],
            faker.state_abbr()[:80],
            faker.state()[:80],
            faker.country()[:80],
            faker.zipcode()[:10],
            faker.building_number()[:10],
            faker.building_number()[:80],
            datetime.now().isoformat()
        ])

    return rows


def upload_product_file(dealer_id: str, salesforce_id: str, product: str):
    rows = generate_rows(dealer_id, salesforce_id)

    s3_key = f"customer-inbound/{product}/{salesforce_id}_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.csv"

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(PRODUCT_COLUMNS)
    writer.writerows(rows)

    s3_client = boto3.client('s3')

    # Upload the in-memory CSV to S3
    s3_client.put_object(Bucket='cdpi-shared-us-east-1-test', Key=s3_key, Body=csv_buffer.getvalue())


if __name__ == '__main__':
    upload_product_file(
        dealer_id='impel-test-dealer',
        salesforce_id='11110000',
        product='service-ai'
    )
