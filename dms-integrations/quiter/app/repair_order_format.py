"""Logic for formatting files for “Repair Orders,” “Customers,” and “Vehicles” from Quiter."""

import os
from os import environ
import logging
import boto3
from json import loads
import datetime
import pandas as pd
import io
import urllib
from botocore.exceptions import ClientError
from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
lambda_client = boto3.client("lambda")

INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")
UNIFIED_RO_INSERT_LAMBDA = os.getenv("UNIFIED_RO_INSERT_LAMBDA")
REGION = environ.get("REGION", "us-east-1")

def send_sns_notification(message, topic_arn):
    """Sends a notification to the SNS topic."""
    try:
        logger.info(f"Sending SNS notification to topic: {topic_arn}")
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject="Unified Repair Order Processing",
        )
        logger.info("SNS notification sent successfully")
    except ClientError as e:
        logger.error(f"Error sending SNS notification: Topic={topic_arn}, Error={e}")
        raise


def default_get(row, key, default_value=None):
    """Return a default value if a column doesn't exist or if its value is None."""
    if key in row and pd.notna(row[key]):
        return row[key]
    return default_value


def convert_to_timestamp(date_str):
    """Convert a date string in 'day/month/year' format to a datetime object"""
    try:
        # Convert the string '13/05/24' to 'YYYY-MM-DD' format
        return datetime.strptime(date_str, "%d/%m/%y").strftime("%Y-%m-%d")
    except ValueError:
        logger.error(f"Error parsing date: {date_str}")
        return None


def parse_csv_to_entries(csv_data, s3_uri):
    """Format CSV data to unified format similar to the XML parsing method."""
    entries = []
    dms_id = None

    # Metadata information based on S3 URI
    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_uri.split("/")[2],
        "PartitionMonth": s3_uri.split("/")[3],
        "PartitionDate": s3_uri.split("/")[4],
        "s3_url": s3_uri,
    }

    # Iterate over each row in the CSV DataFrame
    for _, entry in csv_data.iterrows():
        db_dealer_integration_partner = {}
        db_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        # Get dealer integration partner info (DMS ID)
        dms_id = default_get(entry, "Dealer ID")
        db_dealer_integration_partner["dms_id"] = dms_id

        # Extract Repair Order Information
        db_repair_order["repair_order_no"] = default_get(entry, "Repair Order No")
        db_repair_order["ro_open_date"] = convert_to_timestamp(default_get(entry, "RO Open Date"))
        db_repair_order["advisor_name"] = default_get(entry, "Advisor Name")
        db_repair_order["internal_total_amount"] = default_get(entry, "Internal Total Amount")
        db_repair_order["consumer_total_amount"] = default_get(entry, "Consumer Total Amount")
        db_repair_order["warranty_total_amount"] = default_get(entry, "Warranty Total Amount")

        # Calculate total amount for the repair order
        internal_total = float(db_repair_order["internal_total_amount"] or 0)
        consumer_total = float(db_repair_order["consumer_total_amount"] or 0)
        warranty_total = float(db_repair_order["warranty_total_amount"] or 0)
        db_repair_order["total_amount"] = internal_total + consumer_total + warranty_total

        # Extract Vehicle Information
        db_vehicle["vin"] = default_get(entry, "Vin No")
        db_vehicle["mileage"] = default_get(entry, "Mileage on Vehicle")
        db_vehicle["make"] = default_get(entry, "Make")
        db_vehicle["model"] = default_get(entry, "Model")
        db_vehicle["year"] = default_get(entry, "Year")
        db_vehicle["warranty_expiration_miles"] = default_get(entry, "Warranty Expiration Miles")
        db_vehicle["warranty_expiration_date"] = default_get(entry, "Warranty Expiration Date")

        # Extract Consumer Information
        db_consumer["first_name"] = default_get(entry, "First Name")
        db_consumer["last_name"] = default_get(entry, "Last Name")
        db_consumer["email"] = default_get(entry, "Email")
        db_consumer["cell_phone"] = default_get(entry, "Cell Phone")
        db_consumer["postal_code"] = default_get(entry, "Postal Code")
        db_consumer["home_phone"] = default_get(entry, "Home Phone")
        db_consumer["email_optin_flag"] = default_get(entry, "Email Optin Flag")

        # Extract Operation Codes (Op Codes) Information
        op_code = default_get(entry, "Op Code")
        op_code_desc = default_get(entry, "Op Code Description")
        if op_code:
            db_op_codes.append({
                "op_code": op_code,
                "op_code_desc": op_code_desc,
            })

        # Combine the metadata into each record
        db_vehicle["metadata"] = db_metadata
        db_consumer["metadata"] = db_metadata
        db_repair_order["metadata"] = db_metadata

        # Create entry for each row
        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "repair_order": db_repair_order,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "op_codes.op_codes": db_op_codes,
        }

        entries.append(entry)

    return entries, dms_id


def lambda_handler(event, context):
    """Transform Quiter deals files."""
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")

            # Process each S3 record in the event
            for s3_record in message.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)

                # Fetch the object from S3
                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                csv_data = response["Body"].read().decode("utf-8") 

                # Use pandas to read the CSV content into a DataFrame
                csv_df = pd.read_csv(io.StringIO(csv_data)) 
                # Process the CSV entries using the modified function
                entries, dms_id = parse_csv_to_entries(csv_df, decoded_key)

                if not dms_id:
                    raise RuntimeError("No dms_id found in the CSV data")

                # Call the upload function with the parsed entries
                upload_unified_json(entries, "repair_order", decoded_key, dms_id)

    except Exception as e:
        logger.exception(f"Error transforming vehicle sale file {event}: {e}")
        raise
