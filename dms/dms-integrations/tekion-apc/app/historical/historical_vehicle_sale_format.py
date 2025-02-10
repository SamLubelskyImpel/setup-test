"""Format tekion historical csv vehicle sale data to unified format."""
import logging
import urllib.parse
from json import dumps, loads
from os import environ
import csv
import boto3
from unified_df import upload_unified_json

REGION = environ.get("REGION", "us-east-1")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
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

    for row in reader:
        db_dealer_integration_partner = {}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}

        db_dealer_integration_partner = {
            'dms_id': dms_id
        }

        db_vehicle_sale["sale_date"] = row.get("Contract_Date")
        db_vehicle_sale["listed_price"] = convert_to_float(row.get("Vehicle_Sale_Price"))
        db_vehicle_sale["mileage_on_vehicle"] = convert_to_int(row.get("Delivery_Odometer"))
        db_vehicle_sale["deal_type"] = row.get("Contract_Type")
        db_vehicle_sale["cost_of_vehicle"] = convert_to_float(row.get("Cost"))
        db_vehicle_sale["oem_msrp"] = convert_to_float(row.get("MSRP"))
        db_vehicle_sale["transaction_id"] = row["Deal_Number"]
        db_vehicle_sale["finance_term"] = row["Contract_Term"]
        db_vehicle_sale["first_payment"] = row["First_Payment_Date"]
        db_vehicle_sale["expected_payoff_date"] = row["Expected_Vehicle_Payoff_Date"]
        db_vehicle_sale["finance_amount"] = row["Total_Finance_Amount"] 
        db_vehicle_sale["residual_value"] = convert_to_float(row.get("Residual_Amount"))
        db_vehicle_sale["monthly_payment_amount"] = convert_to_float(row.get("Monthly_Payment"))
        db_vehicle_sale["lease_mileage_limit"] = convert_to_float(row.get("Total_Mileage_Limit"))
        db_vehicle_sale["cost"] = row["Service_Contract_Cost"]
        db_vehicle_sale["expiration_months"] = row["Service_Contract_Term_in_Months"]
        db_vehicle_sale["expiration_miles"] = row["Service_Contract_Term_in_Miles"]
        db_vehicle_sale["trade_in_value"] = convert_to_float(row.get("Total_Trades_Allowance_Amount"))
        #db_vehicle_sale["assignee_dms_id"] = row["Salesman 1 Number"]
        #db_vehicle_sale["assignee_name"] = row["Salesman 1 Name"]
        db_vehicle_sale["adjustment_on_price"] = row["Adjusted Cost"]
        db_vehicle_sale["has_service_contract"] = True if row["Service_Contract_Cost Email"] else False
        db_vehicle_sale["payoff_on_trade"] = convert_to_float(row.get("Total_Trades_Payoff"))
        db_vehicle_sale["miles_per_year"] = convert_to_int(row.get("Total_Mileage_Limit"))
        db_vehicle_sale["profit_on_sale"] = convert_to_float(row.get("Total_Gross_Profit"))
        db_vehicle_sale["vehicle_gross"] = convert_to_float(row.get("Vehicle_Sale_Price"))
        db_vehicle_sale["delivery_date"] = row["Delivery_Date"]
        db_vehicle_sale["value_at_end_of_lease"] = convert_to_float(row.get("Lease Deprecation Value"))
        db_vehicle_sale["sales_tax"] = convert_to_float(row.get("Total_Taxes"))
        db_vehicle_sale["adjustment_on_price"] = convert_to_float(row.get("Adjusted Cost"))
        db_vehicle_sale["vin"] = row.get("VIN")
        db_vehicle_sale["finance_rate"] = row.get("APR")

        db_vehicle["vin"] = row["VIN"]
        db_vehicle["year"] = convert_to_int(row.get("Model_Year"))
        db_vehicle["make"] = row["Make"]
        db_vehicle["model"] = row["Model"]
        #db_vehicle["oem_name"] = row["make"]
        db_vehicle["type"] = row["Body_Description"]
        db_vehicle["trim"] = row["Trim_Level"]
        db_vehicle["stock_num"] = row["Stock_Number"]
        #db_vehicle["vehicle_class"] = row["bodyclass"]
        db_vehicle["exterior_color"] = row.get["Exterior_Color_Description"]
        #db_vehicle["sale_type"] = row["Sale Type"]
        db_vehicle["mileage"] = convert_to_int(row.get("Trade 1 Odometer"))
        db_vehicle["new_or_used"] = "N" if row["Inventory_Type_New_Used_Flag"] == "NEW" else "U" if row["Inventory_Type_New_Used_Flag"] == "USED" else None

        db_consumer["dealer_customer_no"] = row["Buyer_ID"] if row["Buyer_ID"] else row["Co_Buyer_ID"]
        db_consumer["first_name"] = row["Buyer_First_Name"] if row["Buyer_First_Name"] else row["Co_Buyer_First_Name"]
        db_consumer["last_name"] = row["Buyer_Last_Name"] if row["Buyer_Last_Name"] else row["Co_Buyer_Last_Name"]
        db_consumer["email"] = row["Buyer_Personal_Email_Address"] if row["Buyer_Personal_Email_Address"] else row["Co_Buyer_Personal_Email_Address"]
        db_consumer["cell_phone"] = row["Cell Phone"] if row["Cell Phone"] else row["Co-Buyer Cell Phone"]
        db_consumer["home_phone"] = row["Buyer_Home_Phone_Number"] if row["Buyer_Home_Phone_Number"] else row["Co-Buyer Home"]
        db_consumer["state"] = row["Buyer_Home_Address_Region"] if row["Buyer_Home_Address_Region"] else row["Co_Buyer_Home_Address_Region"]
        #db_consumer["city"] = row["buyer_city"]
        db_consumer["postal_code"] = row["Buyer_Home_Address_Postal_Code"] if row["Buyer_Home_Address_Postal_Code"] else row["Co_Buyer_Home_Address_Postal_Code"]
        db_consumer["address"] = row["Buyer_Home_Address"] if row["Buyer_Home_Address"] else row["Co_Buyer_Home_Address"] 
        db_consumer["email_optin_flag"] = True if row["Block Email"] else False
        db_consumer["phone_optin_flag"] = True if row["Block Phone"] else False
        db_consumer["postal_mail_optin_flag"] = True if row["Block Mail"] else False
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
