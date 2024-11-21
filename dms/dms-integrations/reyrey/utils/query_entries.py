""" Run DMS queries. """
from json import loads
from os import environ

import boto3
import psycopg2

AWS_PROFILE = environ["AWS_PROFILE"]


TABLE = "vehicle_sale"

sm_client = boto3.client("secretsmanager")
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

query_str = None
if TABLE == "vehicle_sale":
    query_str = """
    select * from stage.vehicle_sale vs
    join stage.dealer_integration_partner dip on dip.id = vs.dealer_integration_partner_id
    join stage.dealer d on d.id = dip.dealer_id
    join stage.integration_partner ip on ip.id = dip.integration_partner_id
    join stage.vehicle v on v.id = vs.vehicle_id
    join stage.consumer c on c.id = vs.consumer_id
    where ip.impel_integration_partner_id = 'reyrey';
    """
if TABLE == "service_repair_order":
    query_str = """
    select * from stage.service_repair_order sro
    join stage.dealer_integration_partner dip on dip.id = sro.dealer_integration_partner_id
    join stage.dealer d on d.id = dip.dealer_id
    join stage.integration_partner ip on ip.id = dip.integration_partner_id
    join stage.vehicle v on v.id = sro.vehicle_id
    join stage.consumer c on c.id = sro.consumer_id
    where ip.impel_integration_partner_id = 'reyrey';
    """

cursor = rds_connection.cursor()
cursor.execute(query_str)
results = cursor.fetchall()
for result in results:
    print(result)
