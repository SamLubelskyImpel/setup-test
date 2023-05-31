import boto3
import gzip
import psycopg2
from json import loads
from os import environ, remove

AWS_PROFILE = environ["AWS_PROFILE"]

DESIRED_ID = 8660
TABLE = "vehicle_sale"


sm_client = boto3.client("secretsmanager")
secret_string = loads(
    sm_client.get_secret_value(
        SecretId="prod/DMSDB" if AWS_PROFILE == 'unified-prod' else "test/DMSDB"
    )["SecretString"]
)
rds_connection =  psycopg2.connect(
    user=secret_string["user"],
    password=secret_string["password"],
    host=secret_string["host"],
    port=secret_string["port"],
    database=secret_string["db_name"],
)

query_str = None
if TABLE == "vehicle_sale":
    query_str = f"""
    select * from stage.vehicle_sale vs 
    join stage.dealer_integration_partner dip on dip.id = vs.dealer_integration_partner_id 
    join stage.dealer d on d.id = dip.dealer_id 
    join stage.integration_partner ip on ip.id = dip.integration_partner_id 
    join stage.vehicle v on v.id = vs.vehicle_id 
    join stage.consumer c on c.id = vs.consumer_id 
    where vs.id = {DESIRED_ID};
    """
if TABLE == "service_repair_order":
    query_str = f"""
    select * from stage.service_repair_order sro 
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

column_names = [desc[0] for desc in cursor.description]
print(column_names)

data = [list(result) for result in results][0]
print(data)

metadata = data[column_names.index("metadata")]
s3_url = metadata["s3_url"]
bucket, key = s3_url.split('/',2)[-1].split('/',1)
file_name = s3_url.split('/')[-1]

boto3.client('s3').download_file(Bucket=bucket, Key=key, Filename=file_name)
f = gzip.open(file_name, 'rb')
with open('reyreyraw.xml', 'w+') as r:
    r.write(f.read().decode("utf-8"))
remove(file_name)
