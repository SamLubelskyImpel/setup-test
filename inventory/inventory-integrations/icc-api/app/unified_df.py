import boto3
import logging
from datetime import datetime, timezone
from os import environ
import pandas as pd
from rds_instance import RDSInstance

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")

INVENTORY_BUCKET = environ.get("INVENTORY_BUCKET")

# Ignore tables we don't insert into
IGNORE_POSSIBLE_TABLES = ["inv_dealer_integration_partner"]
# Ignore columns we don't insert into (id, fk, audit columns)
IGNORE_POSSIBLE_COLUMNS = [
    "id",
    "consumer_id",
    "dealer_integration_partner_id",
    "vehicle_id",
    "db_creation_date",
    "db_update_date",
    "dealer_id",
    "db_update_role",
]


def validate_unified_df_columns(df):
    """Validate unified DF format."""
    rds_instance = RDSInstance()
    unified_column_names = rds_instance.get_unified_column_names()
    df_table_names = set()
    df_col_names = set()
    for col in df.columns:
        df_table = str(col).split("|")[0]
        df_table_names.add(df_table)
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
    df = pd.json_normalize(json_list, max_level=1)
    df.columns = [str(col).replace(".", "|") for col in df.columns]
    return df


def upload_unified_json(json_list, provider_dealer_id):
    """Upload dataframe to unified s3 path for insertion."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.now(timezone.utc).strftime(format_string)
    s3_key = f"unified/icc-api/{date_key}/{provider_dealer_id}.json"

    df = convert_unified_df(json_list)
    if len(df) > 0:
        validate_unified_df_columns(df)

        s3_client.put_object(
            Bucket=INVENTORY_BUCKET,
            Key=s3_key,
            Body=df.to_json(orient="records")
        )
        logger.info(f"Uploaded {len(df)} rows for {provider_dealer_id} to {s3_key}")
    else:
        logger.info(f"No data uploaded for {provider_dealer_id}")
