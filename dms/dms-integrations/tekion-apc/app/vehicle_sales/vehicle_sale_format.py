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
        #date = datetime.utcfromtimestamp(contract_date / 1000)
        date = contract_date
    elif delivery_date:
        #date = datetime.utcfromtimestamp(delivery_date / 1000)
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
        "ONE_TIME": 0,
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
    return True if preference_mapping and default_get(preference_mapping, "MARKETING", "").upper() == "YES" else False



def parse_json_to_entries(json_data, s3_uri):
    """Format tekion data to unified format."""
    entries = []
    dms_id = None
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
            "PartitionYear": s3_uri.split("/")[2],
            "PartitionMonth": s3_uri.split("/")[3],
            "PartitionDate": s3_uri.split("/")[4],
            "s3_url": s3_uri,
        }

        service_contracts = default_get(entry, "service_contracts", [])
        vehicles = default_get(entry, "vehicles", [])
        warranties = default_get(entry, "warranties", {})
        salesperson = default_get(entry, "salesperson", {})

        dms_id = default_get(entry, "dms_id")  # Added to payload from parent lambda
        db_dealer_integration_partner["dms_id"] = dms_id

        db_vehicle_sale["transaction_id"] = default_get(entry, "externalDealNumber")

        delivery_date = default_get(entry, "deliveryDate", default_get(entry, "promisedDeliveryDate"))
        delivery_datetime = datetime.strptime(delivery_date, "%Y-%m-%d") if delivery_date else None
        db_vehicle_sale["delivery_date"] = delivery_date

        contract_date = default_get(entry, "contractDate")
        contract_datetime = datetime.strptime(contract_date, '%Y-%m-%d') if contract_date else None
        db_vehicle_sale["sale_date"] = contract_date

        # gross_details = default_get(entry, "grossDetails", {})
        # vehicle_gross = default_get(gross_details, "vehicleGross", {})
        # db_vehicle_sale["vehicle_gross"] = default_get(vehicle_gross, "amount")

        trade_ins = default_get(entry, "trade_ins", [])
        if trade_ins:
            for trade_in in trade_ins:
                db_vehicle_sale["trade_in_value"] = default_get(
                    trade_in, "actualCashValue"
                )
                db_vehicle_sale["payoff_on_trade"] = default_get(trade_in, "tradePayOff")



        fnis = default_get(entry, "service_contracts", [])
        db_vehicle_sale["has_service_contract"] = True if fnis else False
        if fnis:
            for fni in fnis:
                disclosure_type = default_get(fni, "disclosureType")
                if disclosure_type and disclosure_type.upper() == "SERVICE_CONTRACT":
                    db_service_contract = {}
                    db_service_contract[
                        "service_contracts|contract_name"
                    ] = default_get(fni, "name")
                    start_date = default_get(fni, "createdTime")
                    db_service_contract["service_contracts|start_date"] = convert_unix_to_timestamp(start_date)

                    db_service_contract[
                        "service_contracts|expiration_miles"
                    ] = default_get(fni, "coverageMileage")

                    term = default_get(fni, "term", {})
                    term_type = default_get(term, "type")
                    term_value = default_get(term, "value")
                    if term_type and term_type.upper() == "MONTH":
                        db_service_contract[
                            "service_contracts|expiration_months"
                        ] = term_value

                    plan = default_get(fni, "plan", {})

                    db_service_contract["service_contracts|amount"] = default_get(
                        plan, "price"
                    )

                    db_service_contract["service_contracts|cost"] = default_get(
                        plan, "cost"
                    )

                    db_service_contract["service_contracts|deductible"] = default_get(
                        plan, "deductibleAmount"
                    )

                    db_service_contract["service_contracts|extended_warranty"] = dumps(
                        fni
                    )
                    db_service_contract["service_contracts|service_package_flag"] = True

                    db_service_contracts.append(db_service_contract)

        deal_payment = default_get(entry, "payment", {})
        total = default_get(deal_payment, "total", {})
        db_vehicle_sale["sales_tax"] = default_get(total, "taxAmount")

        db_vehicle_sale["deal_type"] = default_get(deal_payment, "paymentType")
        db_vehicle_sale["sale_type"] = default_get(entry, "type")

        yearly_miles = default_get(deal_payment, "yearlyMiles", {})
        db_vehicle_sale["miles_per_year"] = default_get(yearly_miles, "baseMileage")

        apr = default_get(deal_payment, "apr", {})
        db_vehicle_sale["finance_rate"] = default_get(apr, "financingRate")

        payment_options = default_get(deal_payment, "paymentOption", {})
        frequency = default_get(payment_options, "paymentFrequency")

        if frequency.upper() != 'MONTHLY':
            db_vehicle_sale["finance_term"] = calculate_payment_term(default_get(payment_options, "term"), frequency)
        else:
            db_vehicle_sale["finance_term"] = default_get(payment_options, "term")

        db_vehicle_sale["finance_amount"] = default_get(deal_payment, "amountFinanced")

        residual = default_get(deal_payment, "residual", {})
        db_vehicle_sale["residual_value"] = default_get(residual, "totalValue")

        db_vehicle_sale["monthly_payment_amount"] = default_get(deal_payment, "monthlyPaymentBeforeTax")

        lease_mileage = default_get(deal_payment, "yearlyMiles", {})
        db_vehicle_sale["lease_mileage_limit"] = default_get(lease_mileage, "totalMileage")

        first_payment_date = calculate_first_payment_date(deal_payment, contract_datetime, delivery_datetime)
        db_vehicle_sale["first_payment"] = first_payment_date
    
        try:
            expected_payoff_date = calculate_expected_payoff_date(deal_payment, contract_datetime, delivery_datetime)
            db_vehicle_sale["expected_payoff_date"] = expected_payoff_date
        except Exception as e:
            logger.warning(f"Error calculating expected payoff date: {e}")
            db_vehicle_sale["expected_payoff_date"] = None

        vehicles = default_get(entry, "vehicles", [])
        if vehicles:
            for vehicle in vehicles:
                db_vehicle_sale["vin"] = default_get(vehicle, "vin")
                
                specification = default_get(vehicle, "vehicleSpecification", {})

                db_vehicle["vin"] = default_get(vehicle, "vin")
                db_vehicle["make"] = default_get(specification, "make")
                db_vehicle["model"] = default_get(specification, "model")
                db_vehicle["year"] = default_get(specification, "year")

                mileage = default_get(vehicle, "odometerReading", {})
                db_vehicle["mileage"] = default_get(mileage, "value")
                db_vehicle_sale["mileage_on_vehicle"] = default_get(mileage, "value")

                db_vehicle["stock_num"] = default_get(vehicle, "stockId")
                stock_type = default_get(vehicle, "stockType", "")
                if stock_type and stock_type.upper() == "NEW":
                    db_vehicle["new_or_used"] = "N"
                elif stock_type and stock_type.upper() == "USED":
                    db_vehicle["new_or_used"] = "U"
                else:
                    db_vehicle["new_or_used"] = None

                trim_details = default_get(specification, "trimDetails", {})
                db_vehicle["oem_name"] = default_get(specification, "make")
                db_vehicle["trim"] = default_get(trim_details, "trim")
                db_vehicle["type"] = default_get(trim_details, "bodyType")
                db_vehicle["vehicle_class"] = default_get(specification, "bodyClass")

                colors = default_get(specification, "vehicleColors", [])
                for color in colors:
                    if default_get(color, "type", "").upper() == 'EXTERIOR':
                        db_vehicle["exterior_color"] = default_get(color, "color")
                        break

                if warranties:
                    warranty = default_get(warranties, default_get(vehicle, "vehicleInventoryId", ""), {})
                    if warranty:
                        endOdometer = default_get(warranty, "endOdometer", {})
                        db_vehicle["warranty_expiration_miles"] = default_get(endOdometer, "value")
                        db_vehicle["warranty_expiration_date"] = default_get(warranty, "endDate")

                db_retail_price = None
                db_oem_msrp = None
                db_adjustment_on_price = 0
                db_cost_of_vehicle = None
 
                pricing = default_get(vehicle, "pricingDetails", {})
                prices = default_get(pricing, "price", [])
                costs = default_get(pricing, "costs", [])
                adjustments = default_get(vehicle, "costAdjustments", [])
                if prices:
                    for price in prices:
                        price_type = default_get(price, "type")
                        price_amount = default_get(price, "amount")
                        if price_type and price_amount:
                            if price_type.upper() == "RETAIL_PRICE":
                                db_retail_price = price_amount
                            if price_type.upper() == "MSRP":
                                db_oem_msrp = price_amount

                if adjustments:
                    for adj in adjustments:
                        db_adjustment_on_price += default_get(adj, "costAdjustment", 0)

                if costs:
                    for cost in costs:
                        cost_type = default_get(cost, "type")
                        cost_amount = default_get(cost, "amount")
                        if cost_type and cost_amount:
                            if cost_type.upper() == "INVOICE_PRICE":
                                db_cost_of_vehicle = cost_amount
        
                db_vehicle_sale["listed_price"] = db_retail_price 
                db_vehicle_sale["oem_msrp"] = db_oem_msrp
                db_vehicle_sale["adjustment_on_price"] = db_adjustment_on_price
                db_vehicle_sale["cost_of_vehicle"] = db_cost_of_vehicle


        trade_in_vehicles = default_get(entry, "trade_ins", [])
        if trade_in_vehicles:
            trade_in_vehicle = trade_in_vehicles[0]
            vehicle = default_get(trade_in_vehicle, "vehicle", {})
            specification = default_get(vehicle, "vehicleSpecification", {})

            db_trade_in_vehicle["vin"] = default_get(vehicle, "vin")
            db_trade_in_vehicle["make"] = default_get(specification, "make")
            db_trade_in_vehicle["model"] = default_get(specification, "model")
            db_trade_in_vehicle["year"] = default_get(specification, "year")
            db_trade_in_vehicle["stock_num"] = default_get(vehicle, "stockId")

            colors = default_get(specification, "vehicleColors", [])
            for color in colors:
                if default_get(color, "type", "").upper() == 'EXTERIOR':
                    db_trade_in_vehicle["exterior_color"] = default_get(color, "color")
                    break

            mileage = default_get(vehicle, "odometerReading", {})
            db_vehicle["mileage"] = default_get(mileage, "value")

            trim_details = default_get(specification, "trimDetails", {})
            db_trade_in_vehicle["oem_name"] = default_get(specification, "make")
            db_trade_in_vehicle["type"] = default_get(trim_details, "bodyType")
            db_trade_in_vehicle["vehicle_class"] = default_get(specification, "bodyClass")
            db_trade_in_vehicle["trim"] = default_get(trim_details, "trim")

        buyer = default_get(entry, "buyer", {})
        cobuyer = default_get(entry, "cobuyer", {})
        buyer_comms = default_get(entry, "buyer_communications", {})
        cobuyer_comms = default_get(entry, "cobuyer_communications", {})

        customers = [buyer, cobuyer]
        comms = {}
        if customers:
            for customer in customers:
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

                details = default_get(customer, "customerDetails", {})


                if customer_type == "BUYER":
                    name = default_get(details, "name", {})
                elif customer_type == "CO_BUYER":
                    db_target = db_cobuyer_consumer
                    comms = cobuyer_comms

                
                emails = default_get(details, "emailCommunications", [])
                phones = default_get(details, "phoneCommunications", [])
                residences = default_get(details, "residences", [])

                db_target["dealer_customer_no"] = default_get(customer, "id")

                # Business Schema use case
                if not name:
                    name = default_get(default_get(details, "principalOwner", {}), "name", {})
                if not residences:
                    residences = default_get(details, "businessLocation", [])

                db_target["first_name"] = default_get(name, "firstName")
                db_target["last_name"] = default_get(name, "lastName")
              
                db_target["email"] = default_get(emails[0], "email") if emails else None

                db_target["email_optin_flag"] = extract_communication_preference(comms, "emailCommunications")
                db_target["phone_optin_flag"] = extract_communication_preference(comms, "phoneCommunications")
                db_target["postal_mail_optin_flag"] = extract_communication_preference(comms, "postalEmailCommunications")
                db_target["sms_optin_flag"] = extract_communication_preference(comms, "smsCommunications")

                for residence in residences:
                    address = default_get(residence, 'address', {})
                    if default_get(residence, "addressType", "").upper() == "CURRENT":
                        loc_units = default_get(address, "locationUnits", [])
                        for unit in loc_units:
                            loc_type = default_get(unit, "type", "").upper()
                            if loc_type == "CITY":
                                db_target["city"] = default_get(unit, "value")
                            elif loc_type == "STATE":
                                db_target["state"] = default_get(unit, "value")
                        db_target["postal_code"] = default_get(address, "postalCode")
                        address_line1 = default_get(address, "addressLine1")
                        address_line2 = default_get(address, "addressLine2")
                        if address_line1 and address_line2:
                            db_target["address"] = f"{address_line1} {address_line2}"
                        elif address_line1:
                            db_target["address"] = address_line1

                db_cell_phone = None
                db_home_phone = None
                if phones:
                    for phone in phones:
                        phone_type = default_get(phone, "phoneType")
                        phone_number = default_get(default_get(phone, "phone", {}), "completeNumber")
                        if phone_type and phone_number:
                            if phone_type.upper() == "HOME":
                                db_home_phone = phone_number
                            elif phone_type.upper() == "MOBILE":
                                db_cell_phone = phone_number
                db_target["cell_phone"] = db_cell_phone
                db_target["home_phone"] = db_home_phone

        assignee = default_get(entry, "salesperson", {})
        db_vehicle_sale["assignee_dms_id"] = default_get(assignee, "id")
        name_details = default_get(assignee, "userNameDetails", {})
        first_name = default_get(name_details, "firstName", "")
        last_name = default_get(name_details, "lastName", "")
        db_vehicle_sale["assignee_name"] = f"{first_name} {last_name}"
          
        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_vehicle_sale["metadata"] = metadata

        if db_cobuyer_consumer:
            db_cobuyer_consumer["metadata"] = metadata

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
