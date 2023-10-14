""" Check each ReyRey f&i file for vehicle sale vins, check db for their existance. """
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


def list_files_in_bucket(bucket_name, prefix, files=[], continuation_token=None):
    """ List all the files in a given bucket with a given prefix. """
    if continuation_token:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            ContinuationToken=continuation_token
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

db_schema = "prod" if AWS_PROFILE == "unified-prod" else "stage"

query_str = f"""select vs.vin from {db_schema}.vehicle_sale vs 
join {db_schema}.dealer_integration_partner dip on dip.id = vs.dealer_integration_partner_id 
join {db_schema}.dealer d on d.id = dip.dealer_id 
join {db_schema}.integration_partner ip on ip.id = dip.integration_partner_id 
join {db_schema}.vehicle v on v.id = vs.vehicle_id 
join {db_schema}.consumer c on c.id = vs.consumer_id 
where ip.impel_integration_partner_id = 'reyrey'
order by vs.db_creation_date desc;"""

cursor = rds_connection.cursor()
cursor.execute(query_str)
results = cursor.fetchall()

all_db_vins = []
for result in results:
    if result[0]:
        all_db_vins.append(result[0].lower())


bucket = (
    "integrations-us-east-1-prod"
    if AWS_PROFILE == "unified-prod"
    else "integrations-us-east-1-test"
)
prefix = "reyrey/fi_closed_deal/"

file_list = list_files_in_bucket(bucket, prefix)
all_vins = []
for key in file_list:
    file_name = key.split("/")[-1]

    boto3.client("s3").download_file(Bucket=bucket, Key=key, Filename=file_name)
    f = gzip.open(file_name, "rb")
    with open("reyreyraw.xml", "w+") as r:
        r.write(f.read().decode("utf-8"))
    remove(file_name)

    with open("reyreyraw.xml", "r") as f:
        soup = BeautifulSoup(f, "xml")
        transaction_vehicles = soup.find_all("TransactionVehicle")
        for transaction_vehicle in transaction_vehicles:
            vehicles = transaction_vehicle.find_all("Vehicle")
            for vehicle in vehicles:
                vin = vehicle.get("Vin")
                all_vins.append(vin)

i = 0
for vin in all_vins:
    if vin.lower() not in all_db_vins:
        print(vin)
        i += 1
print(f"{i} total missing vehicle sales out of {len(all_db_vins)} checked")
