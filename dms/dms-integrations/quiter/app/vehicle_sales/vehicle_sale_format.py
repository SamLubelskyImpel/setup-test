"""Format raw and merged quiter data to unified format."""
import logging
import urllib.parse
from datetime import datetime
from json import loads
from os import environ
import pandas as pd
import io

import boto3

from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")


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



def parse_csv_to_entries(csv_data):
    """Format CSV data to unified format."""
    entries = []
    dms_id = None

    # Iterate over each row in the CSV DataFrame
    for _, entry in csv_data.iterrows():
        db_dealer_integration_partner = {}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}

        dms_id = default_get(entry, "Dealer ID")
        db_dealer_integration_partner["dms_id"] = dms_id

        # Extract Vehicle Sales Information
        sale_date = default_get(entry, "Sale Date")
        db_vehicle_sale["sale_date"] = convert_to_timestamp(sale_date)
        db_vehicle_sale["listed_price"] = default_get(entry, "Listed Price")
        db_vehicle_sale["sales_tax"] = default_get(entry, "Sales Tax")
        db_vehicle_sale["mileage_on_vehicle"] = default_get(entry, "Mileage on Vehicle")
        db_vehicle_sale["deal_type"] = default_get(entry, "Deal Type")
        db_vehicle_sale["cost_of_vehicle"] = default_get(entry, "Cost of Vehicle")
        db_vehicle_sale["oem_msrp"] = default_get(entry, "OEM MSRP")
        db_vehicle_sale["adjustment_on_price"] = default_get(entry, "Discount on Price")
        db_vehicle_sale["days_in_stock"] = default_get(entry, "Days in Stock")
        db_vehicle_sale["date_of_state_inspection"] = default_get(entry, "Date of State Inspection")
        db_vehicle_sale["trade_in_value"] = default_get(entry, "Trade in Value")
        db_vehicle_sale["value_at_end_of_lease"] = default_get(entry, "Value at end of Lease")
        db_vehicle_sale["miles_per_year"] = default_get(entry, "Miles Per Year")
        db_vehicle_sale["profit_on_sale"] = default_get(entry, "Profit on Sale")
        db_vehicle_sale["vehicle_gross"] = default_get(entry, "Vehicle Gross")
                        
        service_contract_value = default_get(entry, "Service Contract Yes No", "").strip().upper()
        db_vehicle_sale["has_service_contract"] = True if service_contract_value == "Y" else False
        db_vehicle_sale["vin"] = default_get(entry, "Vin No")
        
        # Extract Vehicle Information
        db_vehicle["vin"] = default_get(entry,"Vin No")
        db_vehicle["oem_name"] = default_get(entry,"OEM Name")
        db_vehicle["type"] = default_get(entry,"Vehicle Type")
        db_vehicle["vehicle_class"] = default_get(entry,"Vehicle Class")
        db_vehicle["mileage"] = default_get(entry,"Mileage on Vehicle")
        db_vehicle["make"] = default_get(entry,"Make")
        db_vehicle["model"] = default_get(entry,"Model")
        db_vehicle["year"] = default_get(entry,"Year")
        db_vehicle["new_or_used"] = default_get(entry,"New or Used")
        db_vehicle["type"] = default_get(entry,"Vehicle Type")
        db_vehicle["warranty_expiration_miles"] = default_get(entry,"Warranty Expiration Miles")
        
        # Extract Consumer Information
        db_consumer["first_name"] = default_get(entry,"First Name")
        db_consumer["last_name"] = default_get(entry,"Last Name")
        db_consumer["email"] = default_get(entry,"Email")
        db_consumer["cell_phone"] = default_get(entry,"Cell Phone")
        db_consumer["city"] = default_get(entry, "City")
        db_consumer["state"] = default_get(entry,"State")
        db_consumer["metro"] = default_get(entry,"Metro")
        db_consumer["postal_code"] = default_get(entry,"Postal Code")
        db_consumer["home_phone"] = default_get(entry,"Home Phone")


        db_consumer["phone_optin_flag"] = default_get(entry,"Phone Optin Flag")
        db_consumer["postal_mail_optin_flag"] = default_get(entry,"Postal Mail Optin Flag")
        db_consumer["sms_optin_flag"] = default_get(entry,"SMS Optin Flag")


        db_consumer["master_consumer_id"] = default_get(entry,"Master Consumer ID")
        db_consumer["dealer_customer_no"] = default_get(entry,"Consumer ID")

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "vehicle_sale": db_vehicle_sale,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
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
                csv_df = pd.read_csv(io.StringIO(csv_data),dtype={'Dealer ID': 'string'}) 

                # Process the CSV entries using the modified function
                entries, dms_id = parse_csv_to_entries(csv_df)

                if not dms_id:
                    raise RuntimeError("No dms_id found in the CSV data")
                # Call the upload function with the parsed entries
                upload_unified_json(entries, "fi_closed_deal", decoded_key, dms_id)
                
    except Exception as e:
        logger.exception(f"Error transforming vehicle sale file {event}: {e}")
        raise