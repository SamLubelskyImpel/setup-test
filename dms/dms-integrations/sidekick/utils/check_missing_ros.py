import boto3
import psycopg2
from json import dumps, loads
from io import StringIO
from csv import DictReader
from datetime import datetime
import os
import sys

# Constants
AWS_PROFILE = os.environ.get("AWS_PROFILE")
DB_SCHEMA = "prod" if AWS_PROFILE == "unified-prod" else "stage"
BUCKET = (
    "integrations-us-east-1-prod"
    if AWS_PROFILE == "unified-prod"
    else "integrations-us-east-1-test"
)
PREFIX = "sidekick/repair_order/"

# AWS Clients
sm_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

# Retrieve database credentials
secret_string = loads(
    sm_client.get_secret_value(
        SecretId="prod/RDS/DMS" if AWS_PROFILE == "unified-prod" else "test/RDS/DMS"
    )["SecretString"]
)

# Database connection
rds_connection = psycopg2.connect(
    user=secret_string["user"],
    password=secret_string["password"],
    host=secret_string["host"],
    port=secret_string["port"],
    database=secret_string["db_name"],
)


def list_files_in_bucket(bucket_name, prefix):
    """
    List all the files in a given bucket with a given prefix.
    """
    files = []
    continuation_token = None

    while True:
        params = {'Bucket': bucket_name, 'Prefix': prefix}
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**params)

        if "Contents" in response:
            files.extend(obj["Key"] for obj in response["Contents"])

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    return files


def get_all_db_ro_numbers(date_to_process):
    """
    Get all repair order numbers from the database for a given date.
    """
    year, month, day = date_to_process.split('-')
    query_str = f"""SELECT sro.repair_order_no, sro.metadata
                    FROM {DB_SCHEMA}.service_repair_order sro
                    JOIN {DB_SCHEMA}.dealer_integration_partner dip ON dip.id = sro.dealer_integration_partner_id
                    JOIN {DB_SCHEMA}.dealer d ON d.id = dip.dealer_id
                    JOIN {DB_SCHEMA}.integration_partner ip ON ip.id = dip.integration_partner_id
                    JOIN {DB_SCHEMA}.vehicle v ON v.id = sro.vehicle_id
                    JOIN {DB_SCHEMA}.consumer c ON c.id = sro.consumer_id
                    WHERE ip.impel_integration_partner_id = 'sidekick'
                    AND sro.metadata->>'PartitionYear' = '{year}'
                    AND sro.metadata->>'PartitionMonth' = '{month}'
                    AND sro.metadata->>'PartitionDate' = '{day}'
                    ORDER BY sro.db_creation_date DESC;"""

    with rds_connection.cursor() as cursor:
        cursor.execute(query_str)
        results = cursor.fetchall()

    all_db_ro_nums = [str(result[0]).lstrip("0") for result in results if result[0]]
    return all_db_ro_nums


def get_all_ro_numbers_from_files(bucket, prefix, target_date):
    """
    Get all repair order numbers from the files in S3 for a given date.
    """
    file_list = list_files_in_bucket(bucket, prefix)
    all_ro_numbers = []

    for key in file_list:
        file_date = "-".join(key.split("/")[-4:-1])
        if file_date == target_date:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = response["Body"].read().decode("utf-8")
            csv_data = DictReader(StringIO(csv_content), delimiter="|")
            all_ro_numbers.extend(row["RO ID"] for row in csv_data)

    return all_ro_numbers


def main(date_to_process):
    # Get all RO numbers from the database
    all_db_ro_nums = get_all_db_ro_numbers(date_to_process)

    # Get all RO numbers from S3 files
    all_ro_numbers_from_files = get_all_ro_numbers_from_files(
        BUCKET, PREFIX, date_to_process
    )

    # Find missing RO numbers
    missing_ro_numbers = set(all_ro_numbers_from_files) - set(all_db_ro_nums)

    # Print missing RO numbers
    for ro_num in missing_ro_numbers:
        print(ro_num)

    print(f"{len(missing_ro_numbers)} total missing ROs of {len(all_db_ro_nums)}")

    # Create JSON file with data extracted from CSV
    if missing_ro_numbers:
        json_data = {
            "missing_RO_numbers": list(missing_ro_numbers),
            "date_processed": date_to_process,
            "CSV_files": list_files_in_bucket(BUCKET, PREFIX),
        }
        with open("missing_RO_data.json", "w") as json_file:
            json_file.write(dumps(json_data, indent=4))


if __name__ == "__main__":
    if len(sys.argv) == 2:
        input_date = sys.argv[1]
        try:
            datetime.strptime(input_date, '%Y-%m-%d')
            main(input_date)
        except ValueError:
            if input_date.lower() == "now":
                main(datetime.now().strftime('%Y-%m-%d'))
            else:
                print("Invalid input! Please provide a date in the format YYYY-MM-DD or 'now' for the current date.")
    else:
        print("Usage: python check_missing_ros.py <YYYY-MM-DD | now>")
