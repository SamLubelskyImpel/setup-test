"""Format reyrey xml data to unified format."""
import gzip
import io
import logging
import urllib.parse
import xml.etree.ElementTree as ET
from json import dumps, loads
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


def parse_xml_to_entries(xml_string, s3_uri):
    """Format reyrey xml data to unified format."""
    entries = []
    try:
        root = ET.fromstring(xml_string)
    except Exception:
        logger.exception(f"Unable to parse xml at {s3_uri}")
        raise

    ns = {"ns": "http://www.starstandards.org/STAR"}

    application_area = root.find(".//ns:ApplicationArea", namespaces=ns)

    dealer_number = None
    store_number = None
    area_number = None
    if application_area is not None:
        boid = application_area.find(".//ns:BODId", namespaces=ns).text
        sender = application_area.find(".//ns:Sender", namespaces=ns)
        if sender is not None:
            dealer_number = sender.find(".//ns:DealerNumber", namespaces=ns).text
            store_number = sender.find(".//ns:StoreNumber", namespaces=ns).text
            area_number = sender.find(".//ns:AreaNumber", namespaces=ns).text

    if not dealer_number and not store_number and not area_number:
        raise RuntimeError("Unknown dealer id")
    
    dms_id = f"{store_number}_{area_number}_{dealer_number}"

    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_uri.split("/")[2],
        "PartitionMonth": s3_uri.split("/")[3],
        "PartitionDate": s3_uri.split("/")[4],
        "BODId": boid,
        "s3_url": s3_uri,
    }

    vehicle_sales = root.findall(".//ns:FIDeal", namespaces=ns)
    for vehicle_sale in vehicle_sales:
        db_dealer_integration_partner = {"dms_id": dms_id}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}
        db_service_contracts = []

        buyer = vehicle_sale.find(".//ns:Buyer", namespaces=ns)
        if buyer is not None:
            cust_record = buyer.find(".//ns:CustRecord", namespaces=ns)
            if cust_record is not None:
                contact_info = cust_record.find(".//ns:ContactInfo", namespaces=ns)
                if contact_info is not None:
                    db_consumer["dealer_customer_no"] = contact_info.get("NameRecId")
                    db_consumer["first_name"] = contact_info.get("FirstName")
                    db_consumer["last_name"] = contact_info.get("LastName")

                    phones = contact_info.findall(".//ns:Phone", namespaces=ns)
                    for phone in phones:
                        phone_type = phone.get("Type")
                        phone_number = phone.get("Num")
                        if phone_type == "C":
                            db_consumer["cell_phone"] = phone_number
                        if phone_type == "H":
                            db_consumer["home_phone"] = phone_number

                    address = contact_info.find(".//ns:Address", namespaces=ns)
                    if address is not None:
                        db_consumer["postal_code"] = address.get("Zip")

                    email = contact_info.find(".//ns:Email", namespaces=ns)
                    if email is not None:
                        db_consumer["email"] = email.get("MailTo")

                cust_personals = cust_record.findall(
                    ".//ns:CustPersonal", namespaces=ns
                )
                opt_out = []
                for cust_personal in cust_personals:
                    opt_out.append(cust_personal.get("OptOut") == "Y")
                optin_flag = None if not opt_out else not any(opt_out)
                db_consumer["email_optin_flag"] = optin_flag
                db_consumer["phone_optin_flag"] = optin_flag
                db_consumer["postal_mail_optin_flag"] = optin_flag
                db_consumer["sms_optin_flag"] = optin_flag

        fi_deal_fin = vehicle_sale.find(".//ns:FIDealFin", namespaces=ns)
        if fi_deal_fin is not None:
            db_vehicle_sale["sale_date"] = fi_deal_fin.get("CloseDealDate")
            db_vehicle_sale["deal_type"] = fi_deal_fin.get("Category")
            db_vehicle_sale["delivery_date"] = fi_deal_fin.get("DeliveryDate")

            has_service_contract = []
            warranty_infos = fi_deal_fin.findall(".//ns:WarrantyInfo", namespaces=ns)
            for warranty_info in warranty_infos:
                db_service_contract = {}
                service_conts = warranty_info.findall(
                    ".//ns:ServiceCont", namespaces=ns
                )
                
                service_package_flag = []
                for service_cont in service_conts:
                    # Track all service contracts
                    has_service_contract.append(service_cont.get("ServContYN") == "Y")
                    # Track this specific service contract
                    service_package_flag.append(service_cont.get("ServContYN") == "Y")
                db_service_contract["service_contracts|service_package_flag"] = (
                    False if not service_package_flag else any(service_package_flag)
                )

                veh_extended_warranty = warranty_info.find(".//ns:VehExtWarranty", namespaces=ns)
                if veh_extended_warranty is not None:
                    db_service_contract["service_contracts|warranty_expiration_date"] = veh_extended_warranty.get("ExpirationDate")

                db_service_contracts.append(db_service_contract)
            db_vehicle_sale["has_service_contract"] = (
                False if not has_service_contract else any(has_service_contract)
            )

            finance_info = fi_deal_fin.find(".//ns:FinanceInfo", namespaces=ns)
            if finance_info is not None:
                db_vehicle_sale["finance_rate"] = finance_info.get("EnteredRate")
                db_vehicle_sale["finance_term"] = finance_info.get("Term")
                db_vehicle_sale["finance_amount"] = finance_info.get("AmtFinanced")

                lease_spec = finance_info.find(".//ns:LeaseSpec", namespaces=ns)
                if lease_spec is not None:
                    db_vehicle_sale["miles_per_year"] = lease_spec.get("EstDrvYear")

            trade_ins = fi_deal_fin.findall(".//ns:TradeIn", namespaces=ns)
            trade_in_value = None
            payoff_on_trade = None
            for trade_in in trade_ins:
                actual_cash_value = trade_in.get("ActualCashValue")
                if trade_in_value is None:
                    trade_in_value = actual_cash_value
                elif actual_cash_value is not None:
                    trade_in_value += actual_cash_value

                payoff = trade_in.get("Payoff")
                if payoff_on_trade is None:
                    payoff_on_trade = payoff
                elif payoff is not None:
                    payoff_on_trade += payoff
            db_vehicle_sale["trade_in_value"] = trade_in_value
            db_vehicle_sale["payoff_on_trade"] = payoff_on_trade

            transaction_vehicle = fi_deal_fin.find(
                ".//ns:TransactionVehicle", namespaces=ns
            )
            if transaction_vehicle is not None:
                db_vehicle_sale["cost_of_vehicle"] = transaction_vehicle.get("VehCost")
                db_vehicle_sale["oem_msrp"] = transaction_vehicle.get("MSRP")
                db_vehicle_sale["adjustment_on_price"] = transaction_vehicle.get(
                    "Discount"
                )
                db_vehicle_sale["days_in_stock"] = transaction_vehicle.get(
                    "DaysInStock"
                )
                db_vehicle_sale["date_of_state_inspection"] = transaction_vehicle.get(
                    "InspectionDate"
                )
                db_vehicle["stock_num"] = transaction_vehicle.get("StockID")

                transaction_vehicle_info = transaction_vehicle.find(
                    ".//ns:Vehicle", namespaces=ns
                )
                if transaction_vehicle_info is not None:
                    db_vehicle_sale["vin"] = transaction_vehicle_info.get("Vin")
                    db_vehicle["vin"] = transaction_vehicle_info.get("Vin")
                    db_vehicle["make"] = transaction_vehicle_info.get("VehicleMake")
                    db_vehicle["model"] = transaction_vehicle_info.get("Carline")
                    db_vehicle["year"] = transaction_vehicle_info.get("VehicleYr")

                    transaction_vehicle_detail = transaction_vehicle_info.find(
                        ".//ns:VehicleDetail", namespaces=ns
                    )
                    if transaction_vehicle_detail is not None:
                        db_vehicle_sale[
                            "mileage_on_vehicle"
                        ] = transaction_vehicle_detail.get("OdomReading")
                        db_vehicle["new_or_used"] = transaction_vehicle_detail.get(
                            "NewUsed"
                        )
                        db_vehicle["mileage"] = transaction_vehicle_detail.get(
                            "OdomReading"
                        )
                        db_vehicle["vehicle_class"] = transaction_vehicle_detail.get(
                            "VehClass"
                        )

            recap = fi_deal_fin.find(".//ns:Recap", namespaces=ns)
            if recap is not None:
                reserves = recap.find(".//ns:Reserves", namespaces=ns)
                if reserves is not None:
                    db_vehicle_sale["listed_price"] = reserves.get("VehicleGross")
                    db_vehicle_sale["vehicle_gross"] = reserves.get("VehicleGross")

        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_vehicle_sale["metadata"] = metadata

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
    """Transform reyrey repair order files."""
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                with gzip.GzipFile(fileobj=io.BytesIO(response["Body"].read())) as file:
                    xml_string = file.read().decode("utf-8")
                entries, dms_id = parse_xml_to_entries(xml_string, decoded_key)
                upload_unified_json(
                    entries, "fi_closed_deal", decoded_key, dms_id
                )
    except Exception:
        logger.exception(f"Error transforming reyrey repair order file {event}")
        raise
