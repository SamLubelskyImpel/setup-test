"""Format reyrey xml data to unified format."""
import logging
import boto3
import gzip
import io
import pandas as pd
import urllib.parse
from bs4 import BeautifulSoup
from json import loads, dumps
from os import environ
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")


def parse_reyrey_repair_order_xml(xml_string, s3_url):
    """Format reyrey xml data to unified format."""
    entries = []
    soup = BeautifulSoup(xml_string, "xml")
    application_area = soup.find("ApplicationArea")
    if application_area:
        boid = application_area.find("BODId").contents[0]
        sender = application_area.find("Sender")
        if sender:
            dealer_number = sender.find("DealerNumber").contents[0]
    if not dealer_number:
        raise RuntimeError("Unknown dealer id")

    db_metadata = {
        "Region": REGION,
        "PartitionYear": s3_url.split("/")[2],
        "PartitionMonth": s3_url.split("/")[3],
        "PartitionDate": s3_url.split("/")[4],
        "BODId": boid,
        "s3_url": s3_url
    }

    repair_orders = soup.find_all("RepairOrder")
    for repair_order in repair_orders:
        db_dealer_integration_partner = {
            "dms_id": dealer_number
        }
        db_service_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []
        ro_record = repair_order.find("RoRecord")
        if ro_record:
            rogen = ro_record.find("Rogen")
            if rogen:
                db_service_repair_order["repair_order_no"] = rogen.get("RoNo")
                db_service_repair_order["ro_open_date"] = rogen.get("RoCreateDate")
                db_service_repair_order["advisor_name"] = rogen.get("AdvName")
                db_service_repair_order["internal_total_amount"] = rogen.get("IntrRoTotalAmt")
                db_service_repair_order["consumer_total_amount"] = rogen.get("CustRoTotalAmt")
                db_service_repair_order["warranty_total_amount"] = rogen.get("WarrRoTotalAmt")

                db_vehicle["vin"] = rogen.get("Vin")
                db_vehicle["mileage"] = rogen.get("MileageIn")

                if (db_service_repair_order["internal_total_amount"] is None and
                    db_service_repair_order["consumer_total_amount"] is None and
                    db_service_repair_order["warranty_total_amount"] is None):
                    db_service_repair_order["total_amount"] = None
                else:
                    db_service_repair_order["total_amount"] = sum([
                        0.0 if db_service_repair_order["internal_total_amount"] is None else float(db_service_repair_order["internal_total_amount"]),
                        0.0 if db_service_repair_order["consumer_total_amount"] is None else float(db_service_repair_order["consumer_total_amount"]),
                        0.0 if db_service_repair_order["warranty_total_amount"] is None else float(db_service_repair_order["warranty_total_amount"]),
                    ])

                ro_comment_infos = rogen.find_all("RoCommentInfo")
                for ro_comment_info in ro_comment_infos:
                    comment = ro_comment_info.get("RoComment")
                    if comment:
                        db_service_repair_order.setdefault("comment", []).append(comment)

                tech_recommends = rogen.find_all("TechRecommends")
                for tech_recommend in tech_recommends:
                    recommendation = tech_recommend.get("TechRecommend")
                    if recommendation:
                        db_service_repair_order.setdefault("recommendation", []).append(recommendation)

            ro_labor = ro_record.find("Rolabor")
            if ro_labor:
                ro_amounts = ro_labor.find_all("RoAmts")
                txn_pay_type_arr = set()
                for ro_amount in ro_amounts:
                    pay_type = ro_amount.get("PayType")
                    txn_pay_type_arr.add(pay_type)
                db_service_repair_order["txn_pay_type"] = ",".join(list(txn_pay_type_arr))

                op_code_labor_infos = ro_labor.find_all("OpCodeLaborInfo")
                for op_code_labor_info in op_code_labor_infos:
                    db_op_code = {}
                    db_op_code["op_code|op_code"] = op_code_labor_info.get("OpCode")
                    db_op_code["op_code|op_code_desc"] = op_code_labor_info.get("OpCodeDesc")
                    db_op_codes.append(db_op_code)
        service_vehicle = repair_order.find("ServVehicle")
        if service_vehicle:
            vehicle_service_info = service_vehicle.find("VehicleServInfo")
            if vehicle_service_info:
                db_service_repair_order["ro_close_date"] = vehicle_service_info.get("LastRODate")
                db_vehicle["stock_num"] = vehicle_service_info.get("StockID")
            rr_vehicle = service_vehicle.find("Vehicle")
            if rr_vehicle:
                db_vehicle["make"] = rr_vehicle.get("VehicleMake")
                db_vehicle["model"] = rr_vehicle.get("Carline")
                db_vehicle["year"] = rr_vehicle.get("VehicleYr")

        customer_record = repair_order.find("CustRecord")
        if customer_record:
            contact_info = customer_record.find("ContactInfo")
            if contact_info:
                db_consumer["dealer_customer_no"] = contact_info.get("NameRecId")
                db_consumer["first_name"] = contact_info.get("FirstName")
                db_consumer["last_name"] = contact_info.get("LastName")

                phones = contact_info.find_all("Phone")
                for phone in phones:
                    phone_type = phone.get("Type")
                    phone_number = phone.get("Num")
                    if phone_type == "C":
                        db_consumer["cell_phone"] = phone_number
                    if phone_type == "H":
                        db_consumer["home_phone"] = phone_number

                email = contact_info.find("Email")
                if email:
                    mail_to = email.get("MailTo")
                    db_consumer["email"] = mail_to
                    email_optin_flag = isinstance(mail_to, str) and "@" in mail_to
                    db_consumer["email_optin_flag"] = email_optin_flag

                address = contact_info.find("Address")
                if address:
                    db_consumer["postal_code"] = address.get("Zip")

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "service_repair_order": db_service_repair_order,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "op_codes.op_codes": db_op_codes,
        }
        entries.append(entry)

    df = pd.json_normalize(entries)
    df.columns = [str(col).replace(".", "|") for col in df.columns]
    df["vehicle|metadata"] = dumps(db_metadata)
    df["consumer|metadata"] = dumps(db_metadata)
    df["service_repair_order|metadata"] = dumps(db_metadata)

    if len(df) > 0:
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        original_file = s3_url.split('/')[-1].split('.')[0]
        parquet_name = f"{original_file}_{str(uuid4())}.parquet"
        dealer_integration_path = f"dealer_integration_partner|dms_id={dealer_number}"
        partition_path = f"PartitionYear={db_metadata['PartitionYear']}/PartitionMonth={db_metadata['PartitionMonth']}/PartitionDate={db_metadata['PartitionDate']}"
        s3_key = f"unified/repair_order/reyrey/{dealer_integration_path}/{partition_path}/{parquet_name}"
        s3_client.upload_fileobj(buffer, INTEGRATIONS_BUCKET, s3_key)
        logger.info(f"Uploaded {len(df)} rows for {s3_url} to {s3_key}")
    else:
        logger.info(f"No data uploaded for {s3_url}")


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
                with gzip.GzipFile(fileobj=io.BytesIO(response['Body'].read())) as file:
                    xml_string = file.read().decode('utf-8')
                parse_reyrey_repair_order_xml(xml_string, decoded_key)
    except Exception:
        logger.exception(f"Error transforming reyrey repair order file {event}")
        raise
