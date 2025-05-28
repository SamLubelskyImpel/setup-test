import boto3
import logging
from datetime import datetime
from os import environ
import pandas as pd

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


def convert_unified_df(json_list):
    df = pd.json_normalize(json_list, max_level=1)
    df.columns = [str(col).replace(".", "|") for col in df.columns]
    return df


def upload_unified_json(json_list, provider_dealer_id):
    """Upload dataframe to unified s3 path for insertion."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)
    s3_key = f"unified/coxau/{date_key}/{provider_dealer_id}.json"

    df = convert_unified_df(json_list)
    if len(df) > 0:

        s3_client.put_object(
            Bucket=INVENTORY_BUCKET,
            Key=s3_key,
            Body=df.to_json(orient="records")
        )
        logger.info(f"Uploaded {len(df)} rows for {provider_dealer_id} to {s3_key}")
    else:
        logger.info(f"No data uploaded for {provider_dealer_id}")
