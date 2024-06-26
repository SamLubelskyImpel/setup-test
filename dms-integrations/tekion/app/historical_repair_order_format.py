"""Format tekion historical csv data to unified format."""
import logging
import urllib.parse
from json import dumps, loads
from os import environ
import csv

import boto3

from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")


def parse_csv_to_entries(csv_data, s3_uri):
    entries = []
    entries_lookup = {}
    dms_id = None
    reader = csv.DictReader(csv_data.splitlines())

    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_uri.split("/")[4],
        "PartitionMonth": s3_uri.split("/")[5],
        "PartitionDate": s3_uri.split("/")[6],
        "s3_url": s3_uri,
    }

    for row in reader:
        db_service_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        # dms_id = row['dealerid']
        dms_id = '967'  # ???

        db_dealer_integration_partner = {
            'dms_id': dms_id
        }

        repair_order_no = row['roNo']

        # Check if the repair order has already been processed
        if repair_order_no in entries_lookup:
            matching_entry = entries_lookup[repair_order_no]
            # Create a dictionary for the current operation code and its description
            db_op_code = {
                "op_code|op_code": row['opcode'],
                "op_code|op_code_desc": row['opcodeDescription']
            }
            matching_entry["op_codes.op_codes"].append(db_op_code)
        else:
            # add new repair order
            db_service_repair_order["repair_order_no"] = row["roNo"]
            db_service_repair_order["ro_open_date"] = row["roCreatedTime"]
            db_service_repair_order["ro_close_date"] = row["closedTime"]
            db_service_repair_order["txn_pay_type"] = row["payType"]
            db_service_repair_order["advisor_name"] = row["AdvisorName"]
            db_service_repair_order["total_amount"] = row["amount"]
            db_service_repair_order["comment"] = row["concern"]

            # add new vehicle
            db_vehicle["vin"] = row["vin"]
            db_vehicle["year"] = row["year"]
            db_vehicle["make"] = row["make"]
            db_vehicle["model"] = row["model"]
            db_vehicle["oem_name"] = row["make"]
            db_vehicle["type"] = row["bodyType"]
            db_vehicle["vehicle_class"] = row["bodyClass"]
            db_vehicle["mileage"] = int(row["mileageIn"])
            db_vehicle["new_or_used"] = row["vehicleType"]

            # add new consumer
            db_consumer["first_name"] = row["firstName"]
            db_consumer["last_name"] = row["lastName"]
            db_consumer["email"] = row["email"]
            db_consumer["cell_phone"] = row["mobilePhone"]
            db_consumer["home_phone"] = row["homePhone"]
            db_consumer["state"] = row["state"]
            db_consumer["city"] = row["city"]
            db_consumer["postal_code"] = row["postalCode"]
            db_consumer["address"] = row["streetAddress1"] + row["streetAddress2"]
            db_consumer["email_optin_flag"] = True if row["email"] else False
            db_consumer["sms_optin_flag"] = False

            # add new op code
            db_op_code = {
                "op_code|op_code": row['opcode'],
                "op_code|op_code_desc": row['opcodeDescription']
            }
            db_op_codes.append(db_op_code)

            metadata = dumps(db_metadata)
            db_vehicle["metadata"] = metadata
            db_consumer["metadata"] = metadata
            db_service_repair_order["metadata"] = metadata

            entry = {
                "dealer_integration_partner": db_dealer_integration_partner,
                "service_repair_order": db_service_repair_order,
                "vehicle": db_vehicle,
                "consumer": db_consumer,
                "op_codes.op_codes": db_op_codes,
            }
            entries.append(entry)
            entries_lookup[repair_order_no] = entry

    # Log the first 10 entries. Remove after testing
    for entry in entries[:10]:
        logger.info(f"Entries: {entry}")

    return entries, dms_id


def lambda_handler(event, context):
    """Transform tekion historical repair order files."""
    try:
        logger.info(event)
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                csv_data = response["Body"].read().decode('utf-8')
                entries, dms_id = parse_csv_to_entries(csv_data, decoded_key)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "repair_order", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion historical repair order file {event}")
        raise
