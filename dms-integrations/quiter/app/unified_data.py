import logging
from os import environ
from uuid import uuid4

import boto3
import pandas as pd
from rds_instance import RDSInstance

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")

# Many to 1 tables and many to many tables are represented by array of struct columns
MANY_TO_X_TABLES = ["op_codes", "service_contracts"]
# Ignore tables we don't insert into
IGNORE_POSSIBLE_TABLES = ["dealer_integration_partner"]
# Ignore columns we don't insert into (id, fk, audit columns)
IGNORE_POSSIBLE_COLUMNS = [
    "id",
    "consumer_id",
    "dealer_integration_partner_id",
    "vehicle_id",
    "vehicle_sale_id",
    "db_creation_date",
]


def validate_unified_df_columns(df):
    """Validate unified DF format."""
    rds_instance = RDSInstance(IS_PROD)
    unified_column_names = rds_instance.get_unified_column_names()
    df_table_names = set()
    df_col_names = set()
    for col in df.columns:
        df_table = str(col).split("|")[0]
        df_table_names.add(df_table)
        if df_table in MANY_TO_X_TABLES:
            for array in df[col]:
                for struct in array:
                    for key in struct:
                        df_col_names.add(key)
        else:
            df_col_names.add(col)

    # Validate all columns in the DF exist in the database
    for df_col in df_col_names:
        if df_col not in unified_column_names:
            raise RuntimeError(
                f"DF column {df_col} not found in database {unified_column_names}"
            )

    possible_columns = set()
    for df_table_name in df_table_names:
        for unified_column_name in unified_column_names:
            if unified_column_name.split("|")[0] == df_table_name:
                possible_columns.add(unified_column_name)

    # Validate all columns in the database exist in the DF
    missing_columns = []
    for possible_unified_column in possible_columns:
        possible_table, possible_column = possible_unified_column.split("|")
        if (
            possible_table not in IGNORE_POSSIBLE_TABLES
            and possible_column not in IGNORE_POSSIBLE_COLUMNS
            and possible_unified_column not in df_col_names
        ):
            missing_columns.append(possible_unified_column)
    if missing_columns:
        logger.warning(f"DF missing potential column {missing_columns}")

    # Check for empty columns
    columns_with_no_data = df.columns[df.isna().all()].to_list()
    if columns_with_no_data:
        logger.warning(f"DF columns {columns_with_no_data} contain no data")


def convert_unified_df(json_list):
    df = pd.json_normalize(json_list)
    df.columns = [str(col).replace(".", "|") for col in df.columns]
    return df


def upload_unified_json(json_list, integration_type, source_s3_uri, dms_id):
    """Upload dataframe to unified s3 path for insertion."""
    upload_year = source_s3_uri.split("/")[2]
    upload_month = source_s3_uri.split("/")[3]
    upload_date = source_s3_uri.split("/")[4]
    df = convert_unified_df(json_list)
    if len(df) > 0:
        validate_unified_df_columns(df)
        json_str = df.to_json(orient="records")
        original_file = source_s3_uri.split("/")[-1].split(".")[0]
        parquet_name = f"{original_file}_{str(uuid4())}.json"
        dealer_integration_path = f"dealer_integration_partner|dms_id={dms_id}"
        partition_path = f"PartitionYear={upload_year}/PartitionMonth={upload_month}/PartitionDate={upload_date}"
        s3_key = f"unified/{integration_type}/quiter/{dealer_integration_path}/{partition_path}/{parquet_name}"
        s3_client.put_object(
            Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=json_str
        )
        logger.info(f"Uploaded {len(df)} rows for {source_s3_uri} to {s3_key}")
    else:
        logger.info(f"No data uploaded for {source_s3_uri}")
