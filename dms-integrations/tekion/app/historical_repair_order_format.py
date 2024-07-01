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
        # Convert all keys to lowercase
        normalized_row = {k.lower(): v for k, v in row.items()}

        db_service_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        # dms_id = row['dealerid']
        dms_id = '1086'  # hardcoded for testing

        db_dealer_integration_partner = {
            'dms_id': dms_id
        }

        repair_order_no = normalized_row["rono"]

        # Check if the repair order has already been processed
        if repair_order_no in entries_lookup:
            matching_entry = entries_lookup[repair_order_no]
            # Create a dictionary for the current operation code and its description
            db_op_code = {
                "op_code|op_code": row['opcode'],
                "op_code|op_code_desc": row['opcodedescription']
            }
            matching_entry["op_codes.op_codes"].append(db_op_code)
        else:
            # add new repair order
            db_service_repair_order["repair_order_no"] = repair_order_no
            db_service_repair_order["ro_open_date"] = normalized_row["rocreatedtime"]
            db_service_repair_order["ro_close_date"] = normalized_row["closedtime"]
            db_service_repair_order["txn_pay_type"] = normalized_row["paytype"]
            db_service_repair_order["advisor_name"] = normalized_row["advisorname"]
            db_service_repair_order["total_amount"] = normalized_row["amount"]
            db_service_repair_order["comment"] = normalized_row["concern"]

            # add new vehicle
            db_vehicle["vin"] = normalized_row["vin"]
            db_vehicle["year"] = normalized_row["year"]
            db_vehicle["make"] = normalized_row["make"]
            db_vehicle["model"] = normalized_row["model"]
            db_vehicle["oem_name"] = normalized_row["make"]
            db_vehicle["type"] = normalized_row["bodytype"]
            db_vehicle["vehicle_class"] = normalized_row["bodyclass"]
            db_vehicle["mileage"] = int(float(normalized_row["mileagein"])) if normalized_row["mileagein"] else None
            db_vehicle["new_or_used"] = normalized_row["vehicletype"]

            # add new consumer
            db_consumer["first_name"] = normalized_row["firstname"]
            db_consumer["last_name"] = normalized_row["lastname"]
            db_consumer["email"] = normalized_row["email"]
            db_consumer["cell_phone"] = normalized_row["mobilephone"]
            db_consumer["home_phone"] = normalized_row["homephone"]
            db_consumer["state"] = normalized_row["state"]
            db_consumer["city"] = normalized_row["city"]
            db_consumer["postal_code"] = normalized_row["postalcode"]
            db_consumer["address"] = normalized_row["streetaddress1"] + normalized_row["streetaddress2"]
            db_consumer["email_optin_flag"] = True if normalized_row["email"] else False
            db_consumer["sms_optin_flag"] = False

            # add new op code
            db_op_code = {
                "op_code|op_code": normalized_row['opcode'],
                "op_code|op_code_desc": normalized_row['opcodedescription']
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
