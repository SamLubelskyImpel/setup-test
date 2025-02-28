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
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip().replace(",", "")
        return float(value)
    except (ValueError, TypeError):
        return None

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
        normalized_row = {k.lower(): v for k, v in row.items()}

        db_dealer_integration_partner = {}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}
        db_trade_in_vehicle = {}
        db_cobuyer_consumer = {}

        db_dealer_integration_partner = {
            'dms_id': dms_id
        }

        db_vehicle_sale["sale_date"] = None if normalized_row.get("contract date") == "" else normalized_row.get("contract date")
        db_vehicle_sale["listed_price"] = convert_to_float(normalized_row.get("list price"))
        db_vehicle_sale["mileage_on_vehicle"] = convert_to_int(normalized_row.get("delivery mileage"))
        db_vehicle_sale["deal_type"] = normalized_row.get("deal type")
        db_vehicle_sale["cost_of_vehicle"] = convert_to_float(normalized_row.get("cost"))
        db_vehicle_sale["oem_msrp"] = convert_to_float(normalized_row.get("msrp"))
        db_vehicle_sale["transaction_id"] = normalized_row.get("deal number")
        db_vehicle_sale["finance_term"] = normalized_row.get("term")
        db_vehicle_sale["first_payment"] = None if normalized_row.get("first pay date") == "" else normalized_row.get("first pay date")
        #db_vehicle_sale["expected_payoff_date"] = normalized_row.get("expected_vehicle_payoff_date") if normalized_row.get("expected_vehicle_payoff_date") else none
        db_vehicle_sale["finance_amount"] = normalized_row.get("amount financed")
        db_vehicle_sale["residual_value"] = convert_to_float(normalized_row.get("residual amount"))
        db_vehicle_sale["monthly_payment_amount"] = convert_to_float(normalized_row.get("monthly payment"))
        db_vehicle_sale["lease_mileage_limit"] = convert_to_float(normalized_row.get("mileage"))
        db_vehicle_sale["cost"] = convert_to_float(normalized_row.get("cost"))
        #db_vehicle_sale["expiration_months"] = normalized_row.get("service_contract_term_in_months")
        #db_vehicle_sale["expiration_miles"] = normalized_row.get("service_contract_term_in_miles")
        db_vehicle_sale["assignee_name"] = normalized_row["salesman 1 name"]
        db_vehicle_sale["assignee_dms_id"] = normalized_row["salesman 1 number"]
        #db_vehicle_sale["trade_in_value"] = convert_to_float(normalized_row.get("total_trades_allowance_amount"))
        #db_vehicle_sale["has_service_contract"] = true if normalized_row.get("service_contract_cost") else false
        db_vehicle_sale["payoff_on_trade"] = convert_to_float(normalized_row.get("trade 1 payoff"))
        db_vehicle_sale["miles_per_year"] = convert_to_int(normalized_row.get("mileage"))
        db_vehicle_sale["profit_on_sale"] = convert_to_float(normalized_row.get("gross profit"))
        db_vehicle_sale["vehicle_gross"] = convert_to_float(normalized_row.get("sales price"))
        db_vehicle_sale["delivery_date"] = None if normalized_row.get("delivery date") == "" else normalized_row.get("delivery date")
        db_vehicle_sale["value_at_end_of_lease"] = convert_to_float(normalized_row.get("lease deprecation value"))
        db_vehicle_sale["sales_tax"] = convert_to_float(normalized_row.get("total tax"))
        db_vehicle_sale["adjustment_on_price"] = convert_to_float(normalized_row.get("adjusted cost"))
        db_vehicle_sale["vin"] = normalized_row.get("vin")
        db_vehicle_sale["finance_rate"] = normalized_row.get("apr")
        db_vehicle_sale["sale_type"] = normalized_row.get("sale type")

        db_vehicle["vin"] = normalized_row.get("vin")
        db_vehicle["year"] = convert_to_int(normalized_row.get("year"))
        db_vehicle["make"] = normalized_row.get("make")
        db_vehicle["model"] = normalized_row.get("model")
        db_vehicle["type"] = normalized_row.get("description")
        db_vehicle["trim"] = normalized_row.get("trim")
        db_vehicle["stock_num"] = normalized_row.get("stock number")
        db_vehicle["vehicle_class"] = normalized_row["bodyclass"]
        db_vehicle["exterior_color"] = normalized_row.get("exterior color")
        db_vehicle["mileage"] = convert_to_int(normalized_row.get("trade 1 odometer"))
        db_vehicle["new_or_used"] = "N" if normalized_row.get("new/used") == "NEW" else "U" if normalized_row.get("new/used") == "USED" else None

        db_consumer["dealer_customer_no"] = normalized_row.get("customer number")
        db_consumer["first_name"] = normalized_row.get("first name ")
        db_consumer["last_name"] = normalized_row.get("last name")
        db_consumer["email"] = normalized_row.get("email 1")
        db_consumer["cell_phone"] = normalized_row.get("cell phone")
        db_consumer["home_phone"] = normalized_row.get("home phone")
        db_consumer["state"] = normalized_row.get("state")
        db_consumer["city"] = normalized_row.get("city")
        db_consumer["postal_code"] = normalized_row.get("zip")
        db_consumer["address"] = normalized_row.get("address line 1")
        db_consumer["email_optin_flag"] = True if normalized_row.get("block mail") == 'Y' else False
        db_consumer["phone_optin_flag"] = True if normalized_row.get("block phone") == 'Y' else False
        db_consumer["postal_mail_optin_flag"] = True if normalized_row.get("block mail") == 'Y' else False
        db_consumer["sms_optin_flag"] = True if normalized_row.get("block phone") == 'Y' else False

        db_cobuyer_consumer["dealer_customer_no"] = normalized_row.get("co-buyer customer number")
        db_cobuyer_consumer["first_name"] = normalized_row.get("co-buyer first name")
        db_cobuyer_consumer["last_name"] = normalized_row.get("co-buyer last name")
        db_cobuyer_consumer["email"] = normalized_row.get("co-buyer email 1")
        db_cobuyer_consumer["cell_phone"] = normalized_row.get("co-buyer cell phone")
        db_cobuyer_consumer["home_phone"] = normalized_row.get("co-buyer home phone")
        db_cobuyer_consumer["city"] = normalized_row.get("co-buyer city")
        db_cobuyer_consumer["state"] = normalized_row.get("co-buyer state")
        db_cobuyer_consumer["postal_code"] = normalized_row.get("co-buyer zip")
        db_cobuyer_consumer["address"] = normalized_row.get("co-buyer address line 1")
        db_cobuyer_consumer["email_optin_flag"] = True if normalized_row.get("co buyer block mail") == 'Y' else False
        db_cobuyer_consumer["phone_optin_flag"] = True if normalized_row.get("co buyer block phone") == 'Y' else False
        db_cobuyer_consumer["postal_mail_optin_flag"] = True if normalized_row.get("co buyer block mail") == 'Y' else False
        db_cobuyer_consumer["sms_optin_flag"] = True if normalized_row.get("co buyer block phone") == 'Y' else False

        db_trade_in_vehicle["vin"] = normalized_row.get("trade 1 vin")
        db_trade_in_vehicle["make"] = normalized_row.get("trade 1 make")
        db_trade_in_vehicle["model"] = normalized_row.get("trade 1 model")
        db_trade_in_vehicle["year"] = convert_to_int(normalized_row.get("trade 1 year"))
        db_trade_in_vehicle["mileage"] = convert_to_int(normalized_row.get("trade 1 odometer"))

        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_vehicle_sale["metadata"] = metadata
        db_cobuyer_consumer["metadata"] = metadata
        db_trade_in_vehicle["metadata"] = metadata

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "vehicle_sale": db_vehicle_sale,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "trade_in_vehicle": db_trade_in_vehicle,
            "cobuyer_consumer": db_cobuyer_consumer
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
