"""Format raw tekion data to unified format."""
import logging
import urllib.parse
from datetime import datetime, timedelta
from json import dumps, loads
from os import environ
import boto3
from unified_df import upload_unified_json
from math import ceil
import calendar

REGION = environ.get("REGION", "us-east-1")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def default_get(json_dict, key, default_value=None):
    """Return a default value if a key doesn't exist or if it's value is None."""
    json_value = json_dict.get(key)
    return json_value if json_value is not None else default_value


def convert_unix_to_timestamp(unix_time):
    """Convert unix time to datetime object"""
    if not unix_time or not isinstance(unix_time, int) or unix_time == 0:
        return None
    return datetime.utcfromtimestamp(unix_time / 1000).strftime("%Y-%m-%d")


def calculate_expected_payoff_date(deal_payment, contract_date, delivery_date):
    """Calculate the Expected Payoff Date."""
    first_payment_date_str = calculate_first_payment_date(deal_payment, contract_date, delivery_date)
    first_payment_date = datetime.strptime(first_payment_date_str, "%Y-%m-%d")

    payment_option = default_get(deal_payment, "paymentOption", {})
    payment_value = default_get(payment_option, "term", 0)
    frequency = default_get(payment_option, "paymentFrequency", "").upper()

    frequency_intervals = {
        "BI_WEEKLY": 14,
        "FORTNIGHTLY": 14,
        "ONE_TIME": 0,
        "SEMI_MONTHLY": 15,
        "WEEKLY": 7,
        "YEARLY": 365,
    }

    def add_months(date, months):
        """Add months to a date, considering month lengths."""
        month = date.month - 1 + months
        year = date.year + month // 12
        month = month % 12 + 1
        day = min(date.day, calendar.monthrange(year, month)[1])
        return date.replace(year=year, month=month, day=day)

    def add_quarters(date, quarters):
        """Add quarters (3 months each) to a date."""
        return add_months(date, quarters * 3)

    def add_half_years(date, half_years):
        """Add half-years (6 months each) to a date."""
        return add_months(date, half_years * 6)

    if frequency in frequency_intervals:
        interval_days = frequency_intervals[frequency]
        total_payoff_days = payment_value * interval_days
        expected_payoff_date = first_payment_date + timedelta(days=total_payoff_days)
    elif frequency == "MONTHLY":
        expected_payoff_date = first_payment_date
        for _ in range(payment_value):
            expected_payoff_date = add_months(expected_payoff_date, 1)
    elif frequency == "QUARTERLY":
        expected_payoff_date = first_payment_date
        for _ in range(payment_value):
            expected_payoff_date = add_quarters(expected_payoff_date, 1)
    elif frequency == "SEMI_ANNUALLY":
        expected_payoff_date = first_payment_date
        for _ in range(payment_value):
            expected_payoff_date = add_half_years(expected_payoff_date, 1)
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")
    
    return expected_payoff_date.strftime("%Y-%m-%d")


def calculate_first_payment_date(deal_payment, contract_date, delivery_date):
    """Calculate the First Payment Date"""
    if contract_date:
        date = contract_date
    elif delivery_date:
        date = delivery_date
    else:
        raise ValueError("Neither contract date nor delivery date are provided")

    days_to_first_payment = default_get(deal_payment, "daysToFirstPayment", 0)

    if not isinstance(days_to_first_payment, int):
        raise ValueError("daysToFirstPayment must be an integer")
        
    first_payment_date = date + timedelta(days=days_to_first_payment)

    return first_payment_date.strftime("%Y-%m-%d")

def calculate_payment_term(term, frequency):

    if not term:
        raise ValueError(f"term is missing or none")

    term = int(term)

    frequency_intervals = {
        "BI_WEEKLY": 14,
        "FORTNIGHTLY": 14,
        "ONE_TIME": 1,
        "SEMI_MONTHLY": 15,
        "WEEKLY": 7,
        "YEARLY": 365,
        "QUARTERLY": 91,
        "SEMI_ANNUALLY": 182,
    }

    if frequency in frequency_intervals:
        interval_days = frequency_intervals[frequency]
        term_in_days = term * interval_days
        # divide days by average number of days in 1 month to get the approximate number of months in the term
        return ceil((term_in_days/30.44))

    raise ValueError(f"Unsupported frequency: {frequency}")


def extract_communication_preference(comms, key):
    # TODO: Confirm with product which mapping to use
    communications_obj = default_get(comms, key, [])[0]
    preference_mapping = default_get(default_get(communications_obj, "usagePreference", {}), "preferenceMapping", {})
    return preference_mapping and default_get(preference_mapping, "MARKETING", "").upper() == "YES"


def parse_service_contract(fni, deal_id):
    start_date = default_get(fni, "createdTime")
    term = default_get(fni, "term", {})
    term_type = default_get(term, "type", "").upper()
    term_value = default_get(term, "value")
    plan = default_get(fni, "plan", {})

    db_service_contract = {
        "service_contracts|contract_name": default_get(fni, "name"),
        "service_contracts|start_date": convert_unix_to_timestamp(start_date),
        "service_contracts|expiration_miles": default_get(fni, "coverageMileage"),
        "service_contracts|amount": default_get(plan, "price"),
        "service_contracts|cost": default_get(plan, "cost"),
        "service_contracts|deductible": default_get(plan, "deductibleAmount"),
        "service_contracts|service_package_flag": True,
        "service_contracts|vehicle_sale_id": deal_id
    }

    if term_type == "MONTH":
        db_service_contract["service_contracts|expiration_months"] = term_value

    return db_service_contract

def parse_deal_payment(deal_payment, deal, db_vehicle_sale):
    total = default_get(deal_payment, "total", {})
    yearly_miles = default_get(deal_payment, "yearlyMiles", {})
    apr = default_get(deal_payment, "apr", {})
    payment_options = default_get(deal_payment, "paymentOption", {})
    residual = default_get(deal_payment, "residual", {})

    contract_date = default_get(deal, "contractDate")
    contract_datetime = datetime.strptime(contract_date, '%Y-%m-%d') if contract_date else None

    delivery_date = default_get(deal, "deliveryDate", default_get(deal, "promisedDeliveryDate"))
    delivery_datetime = datetime.strptime(delivery_date, "%Y-%m-%d") if delivery_date else None

    db_vehicle_sale.update({
        "sales_tax": default_get(total, "taxAmount"),
        "deal_type": default_get(deal_payment, "paymentType"),
        "sale_type": default_get(deal, "type"),
        "miles_per_year": default_get(yearly_miles, "baseMileage"),
        "finance_rate": default_get(apr, "financingRate"),
        "finance_amount": default_get(deal_payment, "amountFinanced"),
        "residual_value": default_get(residual, "totalValue"),
        "monthly_payment_amount": default_get(deal_payment, "monthlyPaymentBeforeTax"),
        "lease_mileage_limit": default_get(yearly_miles, "totalMileage"),
    })

    # Handle finance term separately due to conditional logic
    frequency = default_get(payment_options, "paymentFrequency", "").upper()
    term = default_get(payment_options, "term")

    db_vehicle_sale["finance_term"] = (
        calculate_payment_term(term, frequency) if frequency and frequency != "MONTHLY" else term
    )

    # Calculate first and expected payoff dates
    db_vehicle_sale["first_payment"] = calculate_first_payment_date(
        deal_payment, contract_datetime, delivery_datetime
    )

    try:
        db_vehicle_sale["expected_payoff_date"] = calculate_expected_payoff_date(
            deal_payment, contract_datetime, delivery_datetime
        )
    except Exception as e:
        logger.warning(f"Error calculating expected payoff date: {e}")
        db_vehicle_sale["expected_payoff_date"] = None


def parse_vehicle(db_vehicle_sale, db_vehicle, vehicle, specification, warranties={}, is_trade_in=False):
    db_vehicle.update({
        "vin": default_get(vehicle, "vin"),
        "make": default_get(specification, "make"),
        "model": default_get(specification, "model"),
        "year": default_get(specification, "year"),
        "stock_num": default_get(vehicle, "stockId"),
        "oem_name": default_get(specification, "make"),
        "vehicle_class": default_get(specification, "bodyClass")
    })

    # Mileage
    mileage = default_get(vehicle, "odometerReading", {})
    mileage_val = default_get(mileage, "value")
    db_vehicle["mileage"] = mileage_val
    db_vehicle_sale["mileage_on_vehicle"] = mileage_val

    # Stock type
    stock_type = default_get(vehicle, "stockType", "").upper()
    db_vehicle["new_or_used"] = "N" if stock_type == "NEW" else "U" if stock_type == "USED" else None

    # Trim details
    trim_details = default_get(specification, "trimDetails", {})
    db_vehicle.update({
        "trim": default_get(trim_details, "trim"),
        "type": default_get(trim_details, "bodyType")
    })

    # Exterior color
    colors = default_get(specification, "vehicleColors", [])
    for color in colors:
        if default_get(color, "type", "").upper() == "EXTERIOR":
            db_vehicle["exterior_color"] = default_get(color, "color")
            break

    # Sale details
    if not is_trade_in:
        pricing = default_get(vehicle, "pricingDetails", {})
        prices = default_get(pricing, "price", [])
        costs = default_get(pricing, "costs", [])
        adjustments = default_get(vehicle, "costAdjustments", [])

        for price in prices:
            price_type = default_get(price, "type", "").upper()
            price_amount = default_get(price, "amount")
            if not price_type or not price_amount:
                continue
            if price_type == "RETAIL_PRICE":
                db_vehicle_sale["listed_price"] = price_amount
            elif price_type == "MSRP":
                db_vehicle_sale["oem_msrp"] = price_amount

        db_vehicle_sale["adjustment_on_price"] = sum(
            default_get(adj, "costAdjustment", 0) for adj in adjustments
        )

        for cost in costs:
            if default_get(cost, "type", "").upper() == "INVOICE_PRICE":
                db_vehicle_sale["cost_of_vehicle"] = default_get(cost, "amount")
                break

        warranty_id = default_get(vehicle, "vehicleInventoryId", "")
        warranty = default_get(warranties, warranty_id, {})
        end_odo = default_get(warranty, "endOdometer", {})
        db_vehicle["warranty_expiration_miles"] = default_get(end_odo, "value")
        db_vehicle["warranty_expiration_date"] = default_get(warranty, "endDate")


def parse_consumer(db_target, customer, comms):
    details = default_get(customer, "customerDetails", {})

    name = default_get(details, "name", {})
    emails = default_get(details, "emailCommunications", [])
    phones = default_get(details, "phoneCommunications", [])
    residences = default_get(details, "residences", [])

    db_target["dealer_customer_no"] = default_get(customer, "id")

    db_target["first_name"] = default_get(name, "firstName")
    db_target["last_name"] = default_get(name, "lastName")
    
    db_target["email"] = default_get(emails[0], "email") if emails else None

    db_target["email_optin_flag"] = extract_communication_preference(comms, "emailCommunications")
    db_target["phone_optin_flag"] = extract_communication_preference(comms, "phoneCommunications")
    db_target["postal_mail_optin_flag"] = extract_communication_preference(comms, "postalEmailCommunications")
    db_target["sms_optin_flag"] = extract_communication_preference(comms, "smsCommunications")

    for residence in residences:
        if default_get(residence, "addressType", "").upper() != "CURRENT":
            continue

        address = default_get(residence, "address", {})
        loc_units = default_get(address, "locationUnits", [])

        for unit in loc_units:
            loc_type = default_get(unit, "type", "").upper()
            if loc_type == "CITY":
                db_target["city"] = default_get(unit, "value")
            elif loc_type == "STATE":
                db_target["state"] = default_get(unit, "value")

        db_target["postal_code"] = default_get(address, "postalCode")
        addr1 = default_get(address, "addressLine1", "")
        addr2 = default_get(address, "addressLine2", "")
        db_target["address"] = f"{addr1} {addr2}".strip()
        break

    for phone in phones:
        phone_type = default_get(phone, "phoneType", "").upper()
        phone_data = default_get(phone, "phone", {})
        phone_number = default_get(phone_data, "completeNumber")

        if not phone_type or not phone_number:
            continue

        if phone_type == "HOME":
            db_target["home_phone"] = phone_number
        elif phone_type == "MOBILE":
            db_target["cell_phone"] = phone_number


def parse_assignee(db_vehicle_sale, assignee):
    db_vehicle_sale["assignee_dms_id"] = default_get(assignee, "id")
    name_details = default_get(assignee, "userNameDetails", {})
    first_name = default_get(name_details, "firstName", "")
    last_name = default_get(name_details, "lastName", "")
    db_vehicle_sale["assignee_name"] = f"{first_name} {last_name}"
        
def parse_json_to_entries(json_data, s3_uri):
    """Format tekion data to unified format."""
    entries = []
    dms_id = s3_uri.split("/")[3]
    for entry in json_data:
        db_dealer_integration_partner = {}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}
        db_trade_in_vehicle = {}
        db_cobuyer_consumer = {}
        db_service_contracts = []

        db_metadata = {
            "Region": REGION,
            "PartitionYear": s3_uri.split("/")[4],
            "PartitionMonth": s3_uri.split("/")[5],
            "PartitionDate": s3_uri.split("/")[6],
            "s3_url": s3_uri,
        }

        deal = default_get(entry, "deal", {})

        deal_id = default_get(deal, "id")

        db_dealer_integration_partner["dms_id"] = dms_id

        db_vehicle_sale["transaction_id"] = deal_id

        delivery_date = default_get(deal, "deliveryDate", default_get(deal, "promisedDeliveryDate"))
        db_vehicle_sale["delivery_date"] = delivery_date

        contract_date = default_get(deal, "contractDate")
        db_vehicle_sale["sale_date"] = contract_date

        # gross_details = default_get(deal, "grossDetails", {})
        # vehicle_gross = default_get(gross_details, "vehicleGross", {})
        # db_vehicle_sale["vehicle_gross"] = default_get(vehicle_gross, "amount")

        # Parse Service Contracts

        fnis = default_get(entry, "api_service_contracts", [])
        
        for fni in fnis:
            disclosure_type = default_get(fni, "disclosureType", "")
            if disclosure_type.upper() == "SERVICE_CONTRACT":
                db_service_contracts.append(parse_service_contract(fni, deal_id))

        db_vehicle_sale["has_service_contract"] = True if db_service_contracts else False

        # Parse Deal Payment

        deal_payment = default_get(entry, "api_payment", {})
        parse_deal_payment(deal_payment, deal, db_vehicle_sale)

        # Parse Deal Vehicles

        vehicles = default_get(entry, "api_vehicles", [])
        warranties = default_get(entry, "api_warranties", {})
        if vehicles:
            for vehicle in vehicles:
                db_vehicle_sale["vin"] = default_get(vehicle, "vin")
                
                specification = default_get(vehicle, "vehicleSpecification", {})

                parse_vehicle(db_vehicle_sale, db_vehicle, vehicle, specification, warranties)

        # Parse Trade ins

        trade_in_vehicles = default_get(entry, "api_trade_ins", [])
        if trade_in_vehicles:
            trade_in_vehicle = trade_in_vehicles[0]
            vehicle = default_get(trade_in_vehicle, "vehicle", {})
            specification = default_get(vehicle, "vehicleSpecification", {})

            db_vehicle_sale.update({
                "trade_in_value": default_get(trade_in_vehicle, "actualCashValue"),
                "payoff_on_trade": default_get(trade_in_vehicle, "tradePayOff")
            })

            parse_vehicle(db_vehicle_sale, db_trade_in_vehicle, vehicle, specification, is_trade_in=True)

        # Parse Buyer / Cobuyer

        buyer = default_get(entry, "api_buyer", {})
        cobuyer = default_get(entry, "api_cobuyer", {})
        buyer_comms = default_get(entry, "api_buyer_communications", {})
        cobuyer_comms = default_get(entry, "api_cobuyer_communications", {})

        customers = [buyer, cobuyer]
        comms = {}

        for customer in customers:
            if default_get(customer, "customerType", "").upper() == 'BUSINESS':
                logger.warning("Cannot parse business entity as a consumer")
                continue

            customer_type = default_get(customer, "type", "")
                
            if customer_type == "BUYER":
                db_target = db_consumer
                comms = buyer_comms
            elif customer_type == "CO_BUYER":
                db_target = db_cobuyer_consumer
                comms = cobuyer_comms
            else:
                logger.warning(f"Unknown customer type: {customer_type}")
                continue
            parse_consumer(db_target, customer, comms)

        # Parse Salesperson

        assignee = default_get(entry, "api_salesperson", {})
        parse_assignee(db_vehicle_sale, assignee)
          
        metadata = dumps(db_metadata)

        db_metadata["optin_updated"] = True
        consumer_metadata = dumps(db_metadata)

        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = consumer_metadata
    
        db_vehicle_sale["metadata"] = metadata

        if db_cobuyer_consumer:
            db_cobuyer_consumer["metadata"] = consumer_metadata
        if db_trade_in_vehicle:
            db_trade_in_vehicle["metadata"] = metadata

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "vehicle_sale": db_vehicle_sale,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "trade_in_vehicle": db_trade_in_vehicle,
            "cobuyer_consumer": db_cobuyer_consumer,
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
                json_data = loads(response["Body"].read())
                entries, dms_id = parse_json_to_entries(json_data, decoded_key)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "fi_closed_deal", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion deals file {event}")
        raise
