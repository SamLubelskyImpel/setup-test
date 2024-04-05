"""Format sidekick csv data to unified format."""
import logging
import urllib.parse
from datetime import datetime
from json import dumps, loads
from os import environ
import csv
import pandas as pd
from io import StringIO
import re

import boto3

from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")

def parse_csv_to_entries(df, s3_uri):
    """Format sidekick csv data to unified format."""
    entries = []
    entries_lookup = {}
    df.columns = df.columns.str.strip()
    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_uri.split("/")[4],
        "PartitionMonth": s3_uri.split("/")[5],
        "PartitionDate": s3_uri.split("/")[6],
        "s3_url": s3_uri,
    }
    try:
        for index, row in df.iterrows():
            repair_order_no = row['RO ID']
            db_dealer_integration_partner = {}
            db_service_repair_order = {}
            db_vehicle = {}
            db_consumer = {}
            db_op_codes = []

            dms_id = row['Site ID']

            db_dealer_integration_partner = {
                'dms_id': dms_id
            }

            # Check if the repair order has already been processed
            if repair_order_no in entries_lookup:
                matching_entry = entries_lookup[repair_order_no]
                # Create a dictionary for the current operation code and its description
                db_op_code = {
                    "op_code|op_code": row['OP Code'],
                    "op_code|op_code_desc": row['OP Description']
                }
                matching_entry["op_codes.op_codes"].append(db_op_code)
            else:
                # add new repair order
                db_service_repair_order['repair_order_no'] = repair_order_no
                db_service_repair_order["ro_open_date"] = row['RO Open Date']
                db_service_repair_order["ro_close_date"] = row['RO Close Date']
                db_service_repair_order["advisor_name"] = row.get('RO Advisor Name')
                db_service_repair_order["total_amount"] = row['RO Total']
                db_service_repair_order["consumer_total_amount"] = row['RO Total']
                db_service_repair_order["comment"] = row.get('RO Comments', None)

                # add new customer
                db_consumer["dealer_customer_no"] = row['Customer ID']
                db_consumer["first_name"] = row['Customer First Name']
                db_consumer["last_name"] = row['Customer Last Name']
                db_consumer["email"] = row.get('Customer Email')
                db_consumer["city"] = row.get('Customer City')
                db_consumer["state"] = row.get('Customer State')
                zip_code_numeric = pd.to_numeric(row.get('Customer Zip'), errors='coerce')
                if pd.notnull(zip_code_numeric):
                    db_consumer["postal_code"] = str(int(zip_code_numeric))

                phone_patterns = r'(?P<Cell>Cell:(\d+))?.*?(?P<Home>Home:(\d+))?.*?(?P<Work>Work:(\d+))?'

                matches = re.search(phone_patterns, row.get('Customer Phone Numbers'))

                if matches:
                    phone_numbers_extracted = matches.groupdict()
                    db_consumer['cell_phone'] = re.sub(r'\D', '', str(phone_numbers_extracted.get('Cell', '')))
                    db_consumer['home_phone'] = re.sub(r'\D', '', str(phone_numbers_extracted.get('Home', '')))

                db_consumer["email_optin_flag"] = True
                db_consumer["sms_optin_flag"] = True

                # add new vehicle
                db_vehicle["vin"] = row.get('Vehicle VIN')
                db_vehicle["make"] = row.get('Vehicle Make')
                db_vehicle["model"] = row.get('Vehicle Model')
                year = int(pd.to_numeric(row.get('Vehicle Year'), errors='coerce')) if pd.notnull(row.get('Vehicle Year')) else None
                mileage = int(pd.to_numeric(row.get('Vehicle Mileage'), errors='coerce')) if pd.notnull(row.get('Vehicle Mileage')) else None
                db_vehicle["year"] = year
                db_vehicle["mileage"] = mileage
                if mileage and year:
                    current_year = datetime.now().year
                    db_vehicle["new_or_used"] = 'U' if mileage > 3000 and year < current_year else 'N'

                # add new opcode
                db_op_code = {
                    "op_code|op_code": row['OP Code'],
                    "op_code|op_code_desc": row['OP Description']
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
        
        logger.info(f"Entries: {entries}")
        return entries, dms_id

    except Exception as e:
        logger.error(f"Error processing record {s3_uri}: {e}")
        raise


def lambda_handler(event, context):
    """Transform sidekick repair order files."""
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
                csv_content = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')), sep='|')
                entries, dms_id = parse_csv_to_entries(csv_content, decoded_key)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "repair_order", decoded_key, dms_id)
    except Exception as e:
        logger.error(f"Error transforming sidekick repair order file {event}: {e}")
        raise
