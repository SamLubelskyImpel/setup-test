"""Format raw tekion data to unified format."""
import logging
import urllib.parse
from json import dumps, loads
from datetime import datetime
from os import environ

import boto3
from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")


def default_get(json_dict, key, default_value=None):
    """Return a default value if a key doesn't exist or if it's value is None."""
    json_value = json_dict.get(key)
    return json_value if json_value is not None else default_value


def parse_json_to_entries(json_data):
    """Format tekion data to unified format."""
    entries = []
    dms_id = None
    for entry in json_data:
        db_dealer_integration_partner = {}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}
        db_service_contracts = []

        dms_id = default_get(entry, "dms_id")
        db_dealer_integration_partner["dms_id"] = dms_id

        db_vehicle_sale["delivery_date"] = default_get(entry, "deliveryDate") if default_get(entry, "deliveryDate") != 0 else None

        contract_date = default_get(entry, "contractDate")
        if contract_date and contract_date != 0 and isinstance(contract_date, int):
            db_vehicle_sale["sale_date"] = datetime.utcfromtimestamp(contract_date / 1000).strftime('%Y-%m-%d %H:%M:%S')
            print(db_vehicle_sale["sale_date"])

        gross_details = default_get(entry, "grossDetails", {})
        vehicle_gross = default_get(gross_details, "vehicleGross", {})
        db_vehicle_sale["vehicle_gross"] = default_get(vehicle_gross, "amount")

        trade_ins = default_get(entry, "tradeIns", [])
        if trade_ins:
            for trade_in in trade_ins:
                trade_allowance = default_get(trade_in, "tradeAllowance", {})
                db_vehicle_sale["trade_in_value"] = default_get(trade_allowance, "amount")

                trade_payoff = default_get(trade_in, "tradePayOff", {})
                db_vehicle_sale["payoff_on_trade"] = default_get(trade_payoff, "amount")

        deal_payment = default_get(entry, "dealPayment", {})

        fnis = default_get(deal_payment, "fnis", [])
        db_vehicle_sale["has_service_contract"] = True if fnis else False
        if fnis:
            for fni in fnis:
                disclosure_type = default_get(fni, "disclosureType")
                if disclosure_type and disclosure_type.upper() == "SERVICE_CONTRACT":
                    db_service_contract = {}
                    db_service_contract["service_contracts|contract_name"] = default_get(fni, "name")
                    db_service_contract["service_contracts|start_date"] = default_get(fni, "createdTime")

                    mileage = default_get(fni, "mileage", {})
                    db_service_contract["service_contracts|expiration_miles"] = default_get(mileage, "value")

                    term = default_get(fni, "term", {})
                    term_type = default_get(term, "type")
                    term_value = default_get(term, "value")
                    if term_type and term_type.upper() == "MONTH":
                        db_service_contract["service_contracts|expiration_months"] = term_value

                    plan = default_get(fni, "plan", {})

                    price = default_get(plan, "price")
                    db_service_contract["service_contracts|amount"] = default_get(price, "amount")

                    cost = default_get(plan, "cost")
                    db_service_contract["service_contracts|cost"] = default_get(cost, "amount")

                    deductible_amount = default_get(plan, "deductibleAmount", {})
                    db_service_contract["service_contracts|deductible"] = default_get(deductible_amount, "amount")

                    db_service_contract["service_contracts|extended_warranty"] = dumps(fni)
                    db_service_contract["service_contracts|service_package_flag"] = True

                    db_service_contracts.append(db_service_contract)

        total = default_get(deal_payment, "total", {})
        tax_amount = default_get(total, "taxAmount", {})
        db_vehicle_sale["sales_tax"] = default_get(tax_amount, "amount")

        payment_option = default_get(deal_payment, "paymentOption", {})
        db_vehicle_sale["deal_type"] = default_get(payment_option, "type")

        yearly_miles = default_get(deal_payment, "yearlyMiles", {})
        db_vehicle_sale["miles_per_year"] = default_get(yearly_miles, "totalValue")

        apr = default_get(deal_payment, "apr", {})
        db_vehicle_sale["finance_rate"] = default_get(apr, "apr")

        payment_options = default_get(deal_payment, "paymentOption", {})
        db_vehicle_sale["finance_term"] = default_get(payment_options, "value")

        amount_financed = default_get(deal_payment, "amountFinanced", {})
        db_vehicle_sale["finance_amount"] = default_get(amount_financed, "amount")

        gross_details = default_get(entry, "grossDetails", {})
        gross_cap_cost = default_get(gross_details, "grossCapCost", {})
        db_vehicle_sale["cost_of_vehicle"] = default_get(gross_cap_cost, "amount")

        vehicles = default_get(entry, "vehicles", [])
        if vehicles:
            for vehicle in vehicles:
                db_vehicle_sale["vin"] = default_get(vehicle, "vin")
                db_vehicle["vin"] = default_get(vehicle, "vin")
                db_vehicle["make"] = default_get(vehicle, "make")
                db_vehicle["model"] = default_get(vehicle, "model")
                db_vehicle["year"] = default_get(vehicle, "year")

                mileage = default_get(vehicle, "mileage", {})
                db_vehicle["mileage"] = default_get(mileage, "value")
                
                stock_type = default_get(vehicle, "stockType")
                if stock_type and stock_type.upper() == "NEW":
                    db_vehicle["new_or_used"] = "N"
                elif stock_type and stock_type.upper() == "USED":
                    db_vehicle["new_or_used"] = "U"
                else:
                    db_vehicle["new_or_used"] = None

                mileage = default_get(vehicle, "mileage", {})
                db_vehicle_sale["mileage_on_vehicle"] = default_get(mileage, "value")

                trim_details = default_get(vehicle, "trimDetails", {})
                db_vehicle["oem_name"] = default_get(trim_details, "oem")
                db_vehicle["type"] = default_get(trim_details, "bodyType")
                db_vehicle["vehicle_class"] = default_get(trim_details, "bodyClass")

                db_retail_price = None
                db_selling_price = None
                db_oem_msrp = None
                db_adjustment_on_price = None
                db_profit_on_sale = None
                pricing = default_get(vehicle, "pricing", [])
                if pricing:
                    for price in pricing:
                        price_type = default_get(price, "type")
                        price_amount = default_get(price, "amount")
                        if price_type and price_amount:
                            if price_type.upper() == "RETAIL_PRICE":
                                db_retail_price = price_amount
                            if price_type.upper() == "SELLING_PRICE":
                                db_selling_price = price_amount
                            if price_type.upper() == "MSRP":
                                db_oem_msrp = price_amount
                            if price_type.upper() == "TOTAL_ADJUSTMENTS":
                                db_adjustment_on_price = price_amount
                            if price_type.upper() == "PROFIT":
                                db_profit_on_sale = price_amount
                db_vehicle_sale["listed_price"] = db_retail_price if db_retail_price else db_selling_price
                db_vehicle_sale["oem_msrp"] = db_oem_msrp
                db_vehicle_sale["adjustment_on_price"] = db_adjustment_on_price
                db_vehicle_sale["profit_on_sale"] = db_profit_on_sale
        
        customers = default_get(entry, "customers", [])
        if customers:
            for customer in customers:
                db_consumer["first_name"] = default_get(customer, "firstName")
                db_consumer["last_name"] = default_get(customer, "lastName")
                db_consumer["email"] = default_get(customer, "email")

                communication_preferences = default_get(customer, "communicationPreferences", {})

                email_preference = default_get(communication_preferences, "email", {})
                db_consumer["email_optin_flag"] = default_get(email_preference, "isOptInService")

                call_preference = default_get(communication_preferences, "call", {})
                db_consumer["phone_optin_flag"] = default_get(call_preference, "isOptInService")

                addresses = default_get(customer, "addresses", [])
                if addresses:
                    db_consumer["address"] = dumps(addresses)
                    for address in addresses:
                        db_consumer["city"] = default_get(address, "city")
                        db_consumer["state"] = default_get(address, "state")
                
                db_cell_phone = None
                db_home_phone = None
                phones = default_get(customer, "phones", [])
                if phones:
                    for phone in phones:
                        phone_type = default_get(phone, "type")
                        phone_number = default_get(phone, "number")
                        if phone_type and phone_number:
                            if phone_type.upper() == "HOME":
                                db_home_phone = phone_number
                            if phone_type.upper() == "CELL":
                                db_cell_phone = phone_number
                db_consumer["cell_phone"] = db_cell_phone
                db_consumer["home_phone"] = db_home_phone

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "vehicle_sale": db_vehicle_sale,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "service_contracts.service_contracts": db_service_contracts,
        }
        entries.append(entry)
    return entries, dms_id


def lambda_handler(event, context):
    """Transform tekion deals files."""
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                json_data= loads(response["Body"].read())
                entries, dms_id = parse_json_to_entries(json_data)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "fi_closed_deal", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion deals file {event}")
        raise
