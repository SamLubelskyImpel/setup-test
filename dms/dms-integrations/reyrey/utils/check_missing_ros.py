""" Check each ReyRey repair order file for ro numbers, check db for their existance. """
import gzip
from json import loads
from os import environ, remove

import boto3
import psycopg2
from bs4 import BeautifulSoup

AWS_PROFILE = environ["AWS_PROFILE"]

sm_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
secret_string = loads(
    sm_client.get_secret_value(
        SecretId="prod/RDS/DMS" if AWS_PROFILE == "unified-prod" else "test/RDS/DMS"
    )["SecretString"]
)
rds_connection = psycopg2.connect(
    user=secret_string["user"],
    password=secret_string["password"],
    host=secret_string["host"],
    port=secret_string["port"],
    database=secret_string["db_name"],
)

db_schema = "prod" if AWS_PROFILE == "unified-prod" else "stage"


def list_files_in_bucket(bucket_name, prefix, files=[], continuation_token=None):
    """List all the files in a given bucket with a given prefix."""
    if continuation_token:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token
        )
    else:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" in response:
        for obj in response["Contents"]:
            files.append(obj["Key"])

    if not response.get("IsTruncated"):
        return files

    continuation_token = response.get("NextContinuationToken")
    return list_files_in_bucket(bucket_name, prefix, files, continuation_token)


query_str = f"""select sro.repair_order_no  from {db_schema}.service_repair_order sro
join {db_schema}.dealer_integration_partner dip on dip.id = sro.dealer_integration_partner_id
join {db_schema}.dealer d on d.id = dip.dealer_id
join {db_schema}.integration_partner ip on ip.id = dip.integration_partner_id
join {db_schema}.vehicle v on v.id = sro.vehicle_id
join {db_schema}.consumer c on c.id = sro.consumer_id
where ip.impel_integration_partner_id = 'reyrey'
order by sro.db_creation_date desc;"""

cursor = rds_connection.cursor()
cursor.execute(query_str)
results = cursor.fetchall()

all_db_ro_nums = []
for result in results:
    if result[0]:
        ro_num = str(result[0]).lstrip("0")
        all_db_ro_nums.append(ro_num)


bucket = (
    "integrations-us-east-1-prod"
    if AWS_PROFILE == "unified-prod"
    else "integrations-us-east-1-test"
)
prefix = "reyrey/repair_order/"

file_list = list_files_in_bucket(bucket, prefix)
all_ro_numbers = []
for key in file_list:
    file_name = key.split("/")[-1]

    boto3.client("s3").download_file(Bucket=bucket, Key=key, Filename=file_name)
    f = gzip.open(file_name, "rb")
    with open("reyreyraw.xml", "w+") as r:
        r.write(f.read().decode("utf-8"))
    remove(file_name)

    with open("reyreyraw.xml", "r") as f:
        soup = BeautifulSoup(f, "xml")
        repair_order_nums = soup.find_all("Rogen")
        for repair_order_num in repair_order_nums:
            ro_number = repair_order_num.get("RoNo")
            ro_num = str(ro_number).lstrip("0")
            all_ro_numbers.append(ro_num)

i = 0
for ro_num in all_ro_numbers:
    if ro_num not in all_db_ro_nums:
        print(ro_num)
        i += 1
print(f"{i} total missing ros of {len(all_db_ro_nums)}")
