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

dms_consumer_ids = [
    17351574,
    17351575,
    17351576,
    17351577,
    17351578,
    17351579,
    17351580,
    26244888,
    26244889,
    26244890,
    26244891,
    26244892
]

crm_lead_ids = [
    6683,
    170,
    13029,
    13056,
    13066,
    13075,
    13079,
    13081
]


def generate_rows(dealer_id: str, salesforce_id: str, row_count=5, product="sales-ai"):
    """Generate a CSV file with random data."""
    rows: list[dict] = []

    for _ in range(row_count):
        faker = Faker()
        # Create base row with standard columns
        row = {
            "dealer_id": dealer_id,
            "customer_id": str(uuid4()),
            "salesforce_id": salesforce_id,
            "first_name": faker.first_name()[:80],
            "last_name": faker.last_name()[:80],
            "phone_number": faker.phone_number()[:15],
            "email_address": faker.email()[:100],
            "email_optin_flag": random.choice([0, 1]),
            "phone_optin_flag": random.choice([0, 1]),
            "sms_optin_flag": random.choice([0, 1]),
            "address_1": faker.street_address()[:80],
            "address_2": faker.secondary_address()[:80],
            "suite": faker.building_number()[:80],
            "city": faker.city()[:80],
            "areatype": faker.state_abbr()[:80],
            "area": faker.state()[:80],
            "country": faker.country()[:80],
            "zip": faker.zipcode()[:10],
            "zipextra": faker.building_number()[:10],
            "pobox": faker.building_number()[:80],
            "record_date": datetime.now().isoformat()
        }

        # Add product-specific columns
        if product == "sales-ai":
            row["crm_lead_id"] = random.choice(crm_lead_ids)
            row["crm_vendor_name"] = "unified_crm_layer"
        elif product == "service-ai":
            row["dms_consumer_id"] = random.choice(dms_consumer_ids)
            row["dms_vendor_name"] = "impel unified data"
        else:
            raise Exception("Invalid product parameter.")

        rows.append(row)

    return rows


def upload_product_file(dealer_id: str, salesforce_id: str, product: str):
    rows = generate_rows(dealer_id, salesforce_id, product=product)
    s3_key = (f"customer-inbound/{product}/{salesforce_id}_"
              f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.csv")

    # Get all column names (standard + product specific)
    columns = PRODUCT_COLUMNS + (
        ["crm_lead_id", "crm_vendor_name"] if product == "sales-ai"
        else ["dms_consumer_id", "dms_vendor_name"]
    )

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=columns)

    writer.writeheader()
    writer.writerows(rows)

    # Write to local CSV file
    # local_filename = f"customer_data_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.csv"
    # with open(local_filename, 'w', newline='') as csv_file:
    #     csv_file.write(csv_buffer.getvalue())
    # print(f"CSV file written to: {local_filename}")

    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket='cdpi-shared-us-east-1-test',
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )


if __name__ == '__main__':
    upload_product_file(
        dealer_id='impel-test-dealer',
        salesforce_id='11110000',
        product='service-ai'
    )
