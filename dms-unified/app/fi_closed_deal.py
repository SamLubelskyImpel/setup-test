"""Insert unified fi deal records."""
import logging
import boto3
import re
from io import BytesIO
from json import loads
from os import environ
import pandas as pd
import urllib.parse
from rds_instance import RDSInstance

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"
s3_client = boto3.client("s3")


def insert_fi_deal_parquet(key, bucket):
    """ Insert to vehicle sale table and linked tables. """
    integration = key.split("/")[2]
    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(s3_obj['Body'].read()))
    if len(df) == 0:
        logger.info(f"No rows for {key}")
        return
    rds = RDSInstance(IS_PROD, integration)
    dms_id = re.search(r"dms_id=(.*?)/PartitionYear", key).group(1)
    db_dealer_integration_partner_id = (
        rds.select_db_dealer_integration_partner_id(dms_id)
    )

    df["consumer|dealer_integration_partner_id"] = db_dealer_integration_partner_id
    df["vehicle|dealer_integration_partner_id"] = db_dealer_integration_partner_id
    df["vehicle_sale|dealer_integration_partner_id"] = db_dealer_integration_partner_id

    inserted_consumer_ids = rds.insert_table_from_df(df, "consumer")
    df["vehicle_sale|consumer_id"] = inserted_consumer_ids

    inserted_vehicle_ids = rds.insert_table_from_df(df, "vehicle")
    df["vehicle_sale|vehicle_id"] = inserted_vehicle_ids

    additional_vehicle_sale_query = "ON CONFLICT ON CONSTRAINT unique_vehicle_sale DO NOTHING"
    inserted_vehicle_sale_ids = rds.insert_table_from_df(df, "vehicle_sale", additional_query=additional_vehicle_sale_query)

    service_contracts_columns = [x.split("|")[1] for x in df.columns if x.startswith("service_contracts|")]
    if service_contracts_columns:
        df["service_contracts|vehicle_sale_id"] = inserted_vehicle_sale_ids
        df["service_contracts|dealer_integration_partner_id"] = db_dealer_integration_partner_id
        inserted_service_contracts_ids = rds.insert_table_from_df(df, "service_contracts")
    
    notification_message = {
        "dealer_integration_partner_id": db_dealer_integration_partner_id,
        "dms_id": dms_id,
        "table_inserted": "vehicle_sale",
        "ids_inserted": inserted_vehicle_sale_ids
    }


def lambda_handler(event: dict, context: dict):
    """ Insert unified fi deal records into the DMS database. """
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                logger.info(f"Parsing {decoded_key}")
                insert_fi_deal_parquet(decoded_key, bucket)
    except Exception as e:
        logger.exception("Error inserting fi deal DMS records")
        raise e
