"""Insert unified repair order records."""
import logging
import boto3
import psycopg2
import re
from io import BytesIO
from json import loads
from os import environ
import pandas as pd
import numpy as np

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"
s3_client = boto3.client("s3")


class RDSInstance:
    """Manage RDS connection."""
    def __init__(self, is_prod, integration):
        self.integration = integration
        self.is_prod = is_prod
        self.schema = f"{'prod' if self.is_prod else 'stage'}"
        self.rds_connection = self.get_rds_connection()

    def get_rds_connection(self):
        """Get connection to RDS database."""
        sm_client = boto3.client("secretsmanager")
        secret_string = loads(
            sm_client.get_secret_value(
                SecretId="prod/DMSDB" if self.is_prod else "test/DMSDB"
            )["SecretString"]
        )
        return psycopg2.connect(
            user=secret_string["user"],
            password=secret_string["password"],
            host=secret_string["host"],
            port=secret_string["port"],
            database=secret_string["db_name"],
        )
    
    def execute_rds(self, query_str):
        """Execute query on RDS and return cursor."""
        cursor = self.rds_connection.cursor()
        cursor.execute(query_str)
        return cursor

    def commit_rds(self, query_str):
        """Execute and commit query on RDS and return cursor."""
        cursor = self.execute_rds(query_str)
        self.rds_connection.commit()
        return cursor
    
    def select_db_dealer_integration_partner_id(self, dms_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_integration_partner_id_query = f"""
            select dip.id from {self.schema}."dealer_integration_partner" dip
            join {self.schema}."integration_partner" i on dip.integration_partner_id = i.id 
            where dip.dms_id = '{dms_id}' and i.impel_integration_partner_id = '{self.integration}' and dip.is_active = true;"""
        results = self.execute_rds(db_dealer_integration_partner_id_query).fetchone()
        if results is None:
            raise RuntimeError(
                f"No active dealer {dms_id} found with query {db_dealer_integration_partner_id_query}."
            )
        else:
            return results[0]

    def get_multi_insert_query(self, records, columns, table_name, additional_query=""):
        """Commit several records to the database.
        columns is an array of strings of the column names to insert into
        records is an array of tuples in the same order as columns
        table_name is the 'schema."table_name"'
        additional_query is any query text to append after the insertion
        """
        if len(records) >= 1:
            cursor = self.rds_connection.cursor()
            values_str = f"({', '.join('%s' for _ in range(len(records[0])))})"
            args_str = ",".join(
                cursor.mogrify(values_str, x).decode("utf-8") for x in records
            )
            columns_str = ", ".join(columns)
            query = f"""INSERT INTO {table_name} ({columns_str}) VALUES {args_str} {additional_query}"""
            return query
        else:
            return None

    def get_insert_query_from_df(self, df, table, additional_query=""):
        """Get query from df where df column names are db column names for the given table."""
        selected_columns = []
        db_columns = []
        for table_to_column_name in df.columns:
            if table_to_column_name.split("|")[0] == table:
                selected_columns.append(table_to_column_name)
                db_columns.append(table_to_column_name.split("|")[1])

        column_data = []
        for _, row in df[selected_columns].replace(np.nan, "NULL").iterrows():
            print(row)
            column_data.append(tuple(row.to_numpy()))
        table_name = f'{self.schema}."{table}"'
        query = self.get_multi_insert_query(
            column_data, db_columns, table_name, additional_query
        )
        return query


def insert_repair_order_parquet(key, bucket):
    """ """
    integration = key.split("/")[2]
    df = pd_read_s3_parquet(key, bucket)
    print(df.columns)
    raise Exception("stop")
    rds = RDSInstance(IS_PROD, integration)
    dms_id = re.search(r"dms_id=(.*?)/PartitionYear", key).group(1)
    db_dealer_integration_partner_id = (
        rds.select_db_dealer_integration_partner_id(dms_id)
    )
    df["consumer|dealer_integration_partner_id"] = db_dealer_integration_partner_id
    df["vehicle|dealer_integration_partner_id"] = db_dealer_integration_partner_id
    df["service_repair_order|dealer_integration_partner_id"] = db_dealer_integration_partner_id
    #repair_order_tables = ["consumer", "vehicle", "service_repair_order", "op_code", "op_code_repair_order"]
    print(rds.get_insert_query_from_df(df, "consumer"))


def pd_read_s3_parquet(key, bucket):
    """ Convert and s3 parquet file to a pandas df. """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj['Body'].read()))


def lambda_handler(event: dict, context: dict):
    """ Insert unified repair order records into the DMS database. """
    try:
        for event in [e for e in event["Records"]]:
            message = loads(event["body"])
            logger.info(f"Message of {message}")
            message = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2023-08-15T20:32:18.516Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'AWS:AROASC67TAR3D4KLGVS5R:GlueJobRunnerSession'}, 'requestParameters': {'sourceIPAddress': '10.128.240.16'}, 'responseElements': {'x-amz-request-id': 'APDHW7VVTH1MT6NQ', 'x-amz-id-2': 'vcne6FhFyUCro9oFQ3sec70QOBWNYcDGWE5/WWGAhza358tqE0faB98mJYZ56NXTa4n1z/AQZcC1BWTBoF328oF/uB6AQWyv'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': '5933b668-c13e-470b-b1d1-8600497bd9e7', 'bucket': {'name': 'integrations-us-east-1-test', 'ownerIdentity': {'principalId': 'A1CIGQGEFG8AA7'}, 'arn': 'arn:aws:s3:::integrations-us-east-1-test'}, 'object': {'key': 'unified/repair_order/reyrey/dealer_integration_partner|dms_id=187451325514506/PartitionYear=2023/PartitionMonth=8/PartitionDate=10/part-00014-5972805f-c50f-4f99-b8d4-21cfbce6e35f.c000.snappy.parquet', 'size': 10060, 'eTag': '7f64ece9d8f215b80eb5b8370309daa9', 'sequencer': '0064DBE0D275874225'}}}]}
            for record in message["Records"]:
                bucket = record["s3"]["bucket"]["name"]
                key = record["s3"]["object"]["key"]
                logger.info(f"Parsing {key}")
                insert_repair_order_parquet(key, bucket)
    except Exception as e:
        logger.exception("Error inserting repair order DMS records")
        raise e

event = {"Records": [{"body": "[]"}]}
lambda_handler(event, 0)
