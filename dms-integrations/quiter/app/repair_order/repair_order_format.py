"""Logic for formatting files for “Repair Orders,” “Customers,” and “Vehicles” from Quiter."""

import os
from os import environ
import logging
import boto3
from json import loads, dumps
from datetime import datetime
import pandas as pd
import io
import urllib
from botocore.exceptions import ClientError
from unified_data import upload_unified_json

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


def default_get(json_dict, key, default_value=None):
    """Return a default value if a key doesn't exist or if the value is None."""
    if json_dict is None:
        return default_value
    json_value = json_dict.get(key)
    return json_value if json_value is not None else default_value


def parse_json_to_entries(json_data):
    """ "Format quiter json data to unified format."""
    entries = []
    dms_id = None
    for repair_order in json_data:
        db_dealer_integration_partner = {}
        db_service_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        dms_id = repair_order.get("Dealer ID")
        db_dealer_integration_partner = {"dms_id": dms_id}

        db_service_repair_order["repair_order_no"] = default_get(repair_order, "Repair Order No")
        ro_open_date = default_get(repair_order, "Repair Order Open Date")
        ro_open_formated_date = datetime.strptime(ro_open_date, '%d/%m/%y').strftime('%Y-%m-%d')

        ro_close_date = default_get(repair_order, "Repair Order Close Date")
        ro_close_formated_date = datetime.strptime(ro_close_date, '%d/%m/%y').strftime('%Y-%m-%d')

        db_service_repair_order["ro_open_date"] = ro_open_formated_date
        db_service_repair_order["ro_close_date"] = ro_close_formated_date

        db_service_repair_order["advisor_name"] = default_get(repair_order, "Advisor Name")
        db_service_repair_order["total_amount"] = default_get(repair_order, "Total Amount")
        db_service_repair_order["consumer_total_amount"] = default_get(repair_order, "Consumer Total Amount")
        db_service_repair_order["warranty_total_amount"] = default_get(repair_order, "Warranty Total Amount")
        db_service_repair_order["txn_pay_type"] = default_get(repair_order, "Txn Pay Type", "")
        db_service_repair_order["comment"] = default_get(repair_order, "Comment")
        db_service_repair_order["recommendation"] = default_get(repair_order, "Recommendation")

        op_codes = default_get(repair_order, "Operation Code", "").split("|")
        op_code_descs = default_get(repair_order, "OP Cde Desc", "").split("|")

        for op_code, op_code_desc in zip(op_codes, op_code_descs):
            db_op_code = {
                "op_code|op_code": op_code,
                "op_code|op_code_desc": op_code_desc[:305]
            }
            db_op_codes.append(db_op_code)

        db_vehicle["vin"] = default_get(repair_order, "Vin No")
        db_vehicle["make"] = default_get(repair_order, "Make")
        db_vehicle["model"] = default_get(repair_order, "Model")
        db_vehicle["year"] = default_get(repair_order, "Year")
        db_vehicle["mileage"] = default_get(repair_order, "Mileage In")
        db_vehicle["type"] = default_get(repair_order, "Vehicle Type")
        db_vehicle["oem_name"] = default_get(repair_order, "OEM Name")
        db_vehicle["warranty_expiration_miles"] = default_get(repair_order, "Warranty Expiration Miles")
        db_vehicle["warranty_expiration_date"] = default_get(repair_order, "Warranty Expiration Date")
        db_vehicle["vehicle_class"] = default_get(repair_order, "Vehicle Class")

        db_consumer["first_name"] = default_get(repair_order, "First Name")
        db_consumer["last_name"] = default_get(repair_order, "Last Name")
        db_consumer["email"] = default_get(repair_order, "Email")
        db_consumer["cell_phone"] = default_get(repair_order, "Cell Phone")
        db_consumer["home_phone"] = default_get(repair_order, "Home Phone")
        db_consumer["city"] = default_get(repair_order, "City")
        db_consumer["state"] = default_get(repair_order, "State")
        db_consumer["postal_code"] = default_get(repair_order, "Postal Code")
        db_consumer["address"] = default_get(repair_order, "Metro", "")
        db_consumer["email_optin_flag"] = default_get(repair_order, "Email Optin Flag")
        db_consumer["phone_optin_flag"] = default_get(repair_order, "Phone Optin Flag")
        db_consumer["postal_mail_optin_flag"] = default_get(repair_order, "Postal Mail Optin Flag")
        db_consumer["sms_optin_flag"] = default_get(repair_order, "SMS Optin Flag")
        db_consumer["master_consumer_id"] = default_get(repair_order, "Master Consumer ID")
        db_consumer["dealer_customer_no"] = default_get(repair_order, "Dealer Customer No")

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "service_repair_order": db_service_repair_order,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "op_codes.op_codes": db_op_codes,
        }
        entries.append(entry)

    return entries, dms_id

def lambda_handler(event, context):
    """Transform Quiter deals files."""
    try:
        logger.info(f"Lambda triggered with event: {event}")
        for record in event["Records"]:
            message = loads(record["body"])

            for s3_record in message.get("Records", []):
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)

                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                logger.info(
                    f"S3 object {decoded_key} fetched successfully from bucket {bucket}"
                )

                csv_data = response["Body"].read().decode("utf-8")
                logger.info(f"CSV data successfully read from S3 object {decoded_key}")

                csv_df = pd.read_csv(io.StringIO(csv_data),dtype={'Dealer ID': 'string'})

                json_data = csv_df.to_json(orient="records")

                logger.info(f"json data {json_data}")

                entries, dms_id = parse_json_to_entries(loads(json_data))

                if not dms_id:
                    logger.error("No dms_id found in the CSV data")
                    raise RuntimeError("No dms_id found in the CSV data")
                logger.info(
                    f"Uploading unified JSON for dms_id {dms_id} and key {decoded_key}"
                )
                upload_unified_json(entries, "repair_order", decoded_key, dms_id)
                logger.info(
                    f"Upload completed for dms_id {dms_id} and key {decoded_key}"
                )

    except Exception as e:
        logger.exception(f"Error transforming vehicle sale file {event}: {e}")
        raise
