""" Given an id and a table use that entries metadata to find the source data file. """

from json import loads
from os import environ

import boto3
import psycopg2

AWS_PROFILE = environ["AWS_PROFILE"]
BUCKET = (
    "integrations-us-east-1-prod"
    if AWS_PROFILE == "unified-prod"
    else "integrations-us-east-1-test"
)
DESIRED_ID = 18967610

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

query_str = f"""
    select sro.id, sro.metadata from stage.service_repair_order sro 
    join stage.dealer_integration_partner dip on dip.id = sro.dealer_integration_partner_id 
    join stage.dealer d on d.id = dip.dealer_id 
    join stage.integration_partner ip on ip.id = dip.integration_partner_id 
    join stage.vehicle v on v.id = sro.vehicle_id 
    join stage.consumer c on c.id = sro.consumer_id 
    where sro.id = {DESIRED_ID};
"""

cursor = rds_connection.cursor()
cursor.execute(query_str)
results = cursor.fetchall()
rds_connection.close()

column_names = [desc[0] for desc in cursor.description]
print(column_names)

data = [list(result) for result in results][0]
print(data)

metadata = data[column_names.index("metadata")]
key = metadata["s3_url"]
file_name = key.split("/")[-1]

boto3.client("s3").download_file(Bucket=BUCKET, Key=key, Filename=file_name)
