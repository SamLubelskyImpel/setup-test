"""Insert unified repair order records."""
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


def insert_repair_order_parquet(key, bucket):
    """ Insert to repair order table and linked tables. """
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
    df["service_repair_order|dealer_integration_partner_id"] = db_dealer_integration_partner_id
    df["op_code|dealer_integration_partner_id"] = db_dealer_integration_partner_id

    inserted_consumer_ids = rds.insert_table_from_df(df, "consumer")
    df["service_repair_order|consumer_id"] = inserted_consumer_ids

    inserted_vehicle_ids = rds.insert_table_from_df(df, "vehicle")
    df["service_repair_order|vehicle_id"] = inserted_vehicle_ids

    service_repair_order_columns = [x.split("|")[1] for x in list(df.columns) if x.startswith("service_repair_order|")]
    additional_service_repair_order_query = f"""ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
            SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, service_repair_order.{x})' for x in service_repair_order_columns])}"""
    service_repair_order_ids = rds.insert_table_from_df(df, "service_repair_order", additional_query=additional_service_repair_order_query)
    df["op_code_repair_order|repair_order_id"] = service_repair_order_ids

    if "op_codes|op_codes" in list(df.columns):
        op_code_df = df.explode("op_codes|op_codes").reset_index(drop=True)
        op_code_df = op_code_df.dropna(subset=["op_codes|op_codes"]).reset_index(drop=True)
        op_code_split_df = pd.DataFrame(op_code_df["op_codes|op_codes"].tolist())
        op_code_df = pd.concat([op_code_df, op_code_split_df], axis=1)
        op_code_df.drop(columns=["op_codes|op_codes"], inplace=True)
        if len(op_code_df) == 0:
            return

        op_code_df_columns = [x.split("|")[1] for x in op_code_df.columns if x.startswith("op_code|")]
        additional_op_code_query = f"""ON CONFLICT ON CONSTRAINT unique_op_code DO UPDATE
                SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, op_code.{x})' for x in op_code_df_columns])}"""
        op_code_df_dedupped = op_code_df[~op_code_df["op_code|op_code"].duplicated(keep="first")]
        rds.insert_table_from_df(op_code_df_dedupped, "op_code", additional_query=additional_op_code_query)

        op_code_repair_order_query = rds.get_op_code_repair_order_query(op_code_df)
        op_code_repair_order_results = rds.commit_rds(op_code_repair_order_query)
        if op_code_repair_order_results is None:
            raise RuntimeError(f"No results from query {op_code_repair_order_query}")
        op_code_repair_order_ids = [x[0] for x in op_code_repair_order_results.fetchall()]
        logger.info(f"Inserted {len(op_code_repair_order_ids)} rows for op_code_repair_order")


def lambda_handler(event: dict, context: dict):
    """ Insert unified repair order records into the DMS database. """
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                # Pyspark auto generates temp files when writing to s3, ignore these files.
                if decoded_key.endswith(".parquet") and decoded_key.split("/")[3] != "_temporary":
                    logger.info(f"Parsing {decoded_key}")
                    insert_repair_order_parquet(decoded_key, bucket)
                else:
                    logger.info(f"Ignore temp pyspark file {decoded_key}")
    except Exception as e:
        logger.exception("Error inserting repair order DMS records")
        raise e
