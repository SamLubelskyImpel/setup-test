""" Check each ReyRey repair order file for ro numbers, check db for their existance. """
import gzip
from json import loads
from os import environ, remove

import boto3
import psycopg2
from bs4 import BeautifulSoup

AWS_PROFILE = environ["AWS_PROFILE"]

sm_client = boto3.client("secretsmanager")
secret_string = loads(
    sm_client.get_secret_value(
        SecretId="prod/DMSDB" if AWS_PROFILE == "unified-prod" else "test/DMSDB"
    )["SecretString"]
)
rds_connection = psycopg2.connect(
    user=secret_string["user"],
    password=secret_string["password"],
    host=secret_string["host"],
    port=secret_string["port"],
    database=secret_string["db_name"],
)


def list_files_in_bucket(bucket_name, prefix):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            files.append(obj["Key"])

    return files


query_str = """select sro.repair_order_no  from stage.service_repair_order sro 
join stage.dealer_integration_partner dip on dip.id = sro.dealer_integration_partner_id 
join stage.dealer d on d.id = dip.dealer_id 
join stage.integration_partner ip on ip.id = dip.integration_partner_id 
join stage.vehicle v on v.id = sro.vehicle_id 
join stage.consumer c on c.id = sro.consumer_id 
where ip.impel_integration_partner_id = 'reyrey'
order by sro.db_creation_date desc;"""

cursor = rds_connection.cursor()
cursor.execute(query_str)
results = cursor.fetchall()

all_db_ro_nums = []
for result in results:
    if result[0] and not result[0].startswith("old_"):
        all_db_ro_nums.append(int(result[0]))


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
            all_ro_numbers.append(int(ro_number))

for ro_num in all_ro_numbers:
    if ro_num not in all_db_ro_nums:
        print(ro_num)
