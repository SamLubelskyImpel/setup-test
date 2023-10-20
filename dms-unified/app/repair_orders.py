"""Insert unified repair order records."""
import logging
import re
import urllib.parse
from io import BytesIO
from json import loads
from os import environ

import boto3
import pandas as pd
from eventbridge import notify_event_bus
from rds_instance import RDSInstance

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"
s3_client = boto3.client("s3")


def insert_repair_order_parquet(key, df):
    """Insert to repair order table and linked tables."""
    integration = key.split("/")[2]
    if len(df) == 0:
        logger.info(f"No rows for {key}")
        return
    rds = RDSInstance(IS_PROD, integration)
    dms_id = re.search(r"dms_id=(.*?)/PartitionYear", key).group(1)
    db_dealer_integration_partner_ids = rds.select_db_dealer_integration_partner_ids(
        dms_id
    )

    if not db_dealer_integration_partner_ids:
        raise RuntimeError(f"Unable to find any reyrey dealers with id {dms_id}")

    # Insert the same data for every dealer who shares the dms_id
    for db_dealer_integration_partner_id in db_dealer_integration_partner_ids:
        logger.info(f"Inserting ROs for {db_dealer_integration_partner_id}")
        df["consumer|dealer_integration_partner_id"] = db_dealer_integration_partner_id
        df["vehicle|dealer_integration_partner_id"] = db_dealer_integration_partner_id
        df[
            "service_repair_order|dealer_integration_partner_id"
        ] = db_dealer_integration_partner_id
        df["op_code|dealer_integration_partner_id"] = db_dealer_integration_partner_id

        # Unique dealer_integration_partner_id, repair_order_no SQL can't insert duplicates
        service_repair_order_unique_constraint = [
            "service_repair_order|dealer_integration_partner_id",
            "service_repair_order|repair_order_no",
        ]
        df = df.drop_duplicates(
            subset=service_repair_order_unique_constraint, keep="first"
        ).reset_index(drop=True)

        inserted_consumer_ids = rds.insert_table_from_df(df, "consumer")
        df["service_repair_order|consumer_id"] = inserted_consumer_ids

        inserted_vehicle_ids = rds.insert_table_from_df(df, "vehicle")
        df["service_repair_order|vehicle_id"] = inserted_vehicle_ids

        service_repair_order_columns = [
            x.split("|")[1]
            for x in list(df.columns)
            if x.startswith("service_repair_order|")
        ]
        additional_service_repair_order_query = f"""ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
                SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, service_repair_order.{x})' for x in service_repair_order_columns])}"""
        service_repair_order_ids = rds.insert_table_from_df(
            df,
            "service_repair_order",
            additional_query=additional_service_repair_order_query,
        )
        df["op_code_repair_order|repair_order_id"] = service_repair_order_ids

        if "op_codes|op_codes" in list(df.columns):
            # Explode op_codes arrays of dict such that each row contains op_code data
            op_code_df = df.explode("op_codes|op_codes").reset_index(drop=True)
            op_code_df = op_code_df.dropna(subset=["op_codes|op_codes"]).reset_index(
                drop=True
            )
            op_code_split_df = pd.DataFrame(op_code_df["op_codes|op_codes"].tolist())
            op_code_df = pd.concat([op_code_df, op_code_split_df], axis=1)
            op_code_df.drop(columns=["op_codes|op_codes"], inplace=True)
            if len(op_code_df) == 0:
                return

            # Insert only unique op codes to avoid insertion error
            op_code_df_columns = [
                x.split("|")[1] for x in op_code_df.columns if x.startswith("op_code|")
            ]
            additional_op_code_query = f"""ON CONFLICT ON CONSTRAINT unique_op_code DO UPDATE
                    SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, op_code.{x})' for x in op_code_df_columns])}"""
            op_code_df_dedupped = op_code_df.copy()
            op_code_unique_constraint = [
                "op_code|op_code",
                "op_code|op_code_desc",
                "op_code|dealer_integration_partner_id",
            ]
            op_code_df_dedupped = op_code_df_dedupped.drop_duplicates(
                subset=op_code_unique_constraint, keep="first"
            ).reset_index(drop=True)
            inserted_op_code_ids = rds.insert_table_from_df(
                op_code_df_dedupped,
                "op_code",
                additional_query=additional_op_code_query,
            )
            op_code_df_dedupped[
                "op_code_repair_order|op_code_id"
            ] = inserted_op_code_ids

            # Add op_code_repair_order|op_code_id to all of the op codes where they match
            op_code_df_columns_full_name = [
                x for x in op_code_df.columns if x.startswith("op_code|")
            ]
            op_code_df = op_code_df.merge(
                op_code_df_dedupped[
                    op_code_df_columns_full_name + ["op_code_repair_order|op_code_id"]
                ],
                on=op_code_df_columns_full_name,
                how="left",
            )

            missing_op_code_id = (
                op_code_df["op_code_repair_order|op_code_id"].isna().any()
            )
            if missing_op_code_id:
                raise RuntimeError(
                    "Some op codes missing op_code_repair_order|op_code_id after inserting and merging"
                )

            rds.insert_table_from_df(op_code_df, "op_code_repair_order")

        notification_message = {
            "impel_integration_partner_id": integration,
            "dealer_integration_partner_id": db_dealer_integration_partner_id,
            "dms_id": dms_id,
            "table_inserted": "service_repair_order",
            "ids_inserted": service_repair_order_ids,
        }

        notify_event_bus(notification_message)
        logger.info(f"Notify {notification_message}")


def lambda_handler(event: dict, context: dict):
    """Insert unified repair order records into the DMS database."""
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                # Pyspark auto generates temp files when writing to s3, ignore these files.
                if (
                    decoded_key.endswith(".parquet")
                    and decoded_key.split("/")[3] != "_temporary"
                ):
                    logger.info(f"Parsing {decoded_key}")
                    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
                    df = pd.read_parquet(BytesIO(s3_obj["Body"].read()))
                    insert_repair_order_parquet(decoded_key, df)
                elif (
                    decoded_key.endswith(".json")
                    and decoded_key.split("/")[3] != "_temporary"
                ):
                    logger.info(f"Parsing {decoded_key}")
                    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
                    df = pd.read_json(loads(s3_obj["Body"].read()))
                    insert_repair_order_parquet(decoded_key, df)
                else:
                    logger.info(f"Ignore temp pyspark file {decoded_key}")
    except Exception as e:
        logger.exception("Error inserting repair order DMS records")
        raise e
