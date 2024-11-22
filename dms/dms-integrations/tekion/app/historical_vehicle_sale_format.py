"""Format tekion historical csv vehicle sale data to unified format."""
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


def convert_to_float(value):
    return float(value) if value and value.replace('.', '', 1).isdigit() else None


def convert_to_int(value):
    return int(float(value)) if value and value.replace('.', '', 1).isdigit() else None


def parse_csv_to_entries(csv_data, s3_uri):
    entries = []
    dms_id = s3_uri.split('/')[3]
    reader = csv.DictReader(csv_data.splitlines())

    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_uri.split("/")[4],
        "PartitionMonth": s3_uri.split("/")[5],
        "PartitionDate": s3_uri.split("/")[6],
        "s3_url": s3_uri,
    }

    try:
        for row in reader:
            db_dealer_integration_partner = {}
            db_vehicle_sale = {}
            db_vehicle = {}
            db_consumer = {}

            db_dealer_integration_partner = {
                'dms_id': dms_id
            }

            db_vehicle_sale["sale_date"] = row.get("dealCreatedTime")
            db_vehicle_sale["listed_price"] = convert_to_float(row.get("retailprice"))
            db_vehicle_sale["mileage_on_vehicle"] = convert_to_int(row.get("mileage"))
            db_vehicle_sale["deal_type"] = row.get("paymenttype")
            db_vehicle_sale["cost_of_vehicle"] = convert_to_float(row.get("finalcost"))
            db_vehicle_sale["oem_msrp"] = convert_to_float(row.get("msrp"))
            db_vehicle_sale["payoff_on_trade"] = convert_to_float(row.get("tradein1_payoff"))
            db_vehicle_sale["miles_per_year"] = convert_to_int(row.get("yrlymiles_basevalue"))
            db_vehicle_sale["profit_on_sale"] = convert_to_float(row.get("profit"))
            db_vehicle_sale["vehicle_gross"] = convert_to_float(row.get("retailprice"))
            db_vehicle_sale["vin"] = row.get("vin")
            db_vehicle_sale["finance_rate"] = row.get("apr")

            db_vehicle["vin"] = row["vin"]
            db_vehicle["year"] = convert_to_int(row.get("year"))
            db_vehicle["make"] = row["make"]
            db_vehicle["model"] = row["model"]
            db_vehicle["oem_name"] = row["make"]
            db_vehicle["type"] = row["bodytype"]
            db_vehicle["vehicle_class"] = row["bodyclass"]
            db_vehicle["mileage"] = convert_to_int(row.get("mileage"))
            db_vehicle["new_or_used"] = "N" if row["vehicletype"] == "NEW" else "U" if row["vehicletype"] == "USED" else None

            db_consumer["first_name"] = row["buyer_firstName"]
            db_consumer["last_name"] = row["buyer_lastName"]
            db_consumer["email"] = row["buyer_email"]
            db_consumer["cell_phone"] = row["buyer_mobilePhone"]
            db_consumer["home_phone"] = row["buyer_homePhone"]
            db_consumer["state"] = row["buyer_state"]
            db_consumer["city"] = row["buyer_city"]
            db_consumer["postal_code"] = row["buyer_postalCode"]
            db_consumer["address"] = row["buyer_streetAddress1"] + row["buyer_streetAddress2"]
            db_consumer["email_optin_flag"] = True if row["buyer_email"] else False
            db_consumer["sms_optin_flag"] = False

            metadata = dumps(db_metadata)
            db_vehicle["metadata"] = metadata
            db_consumer["metadata"] = metadata
            db_vehicle_sale["metadata"] = metadata

            entry = {
                "dealer_integration_partner": db_dealer_integration_partner,
                "vehicle_sale": db_vehicle_sale,
                "vehicle": db_vehicle,
                "consumer": db_consumer
            }
            entries.append(entry)

    except Exception as e:
        logger.exception(f"Error parsing row {row} : {e}")
        raise e

    return entries, dms_id


def lambda_handler(event, context):
    """Transform tekion historical vehicle sale files."""
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
                upload_unified_json(entries, "fi_closed_deal", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion historical repair order file {event}")
        raise
