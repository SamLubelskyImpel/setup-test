"""Format reyrey xml data to unified format."""
import logging
import boto3
import gzip
import io
import pandas as pd
import urllib.parse
import xml.etree.ElementTree as ET
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
    root = ET.fromstring(xml_string)

    ns = {'ns': 'http://www.starstandards.org/STAR'}

    application_area = root.find(".//ns:ApplicationArea", namespaces=ns)

    dealer_number = None
    if application_area is not None:
        boid = application_area.find(".//ns:BODId", namespaces=ns).text
        sender = application_area.find(".//ns:Sender", namespaces=ns)
        if sender is not None:
            dealer_number = sender.find(".//ns:DealerNumber", namespaces=ns).text

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

    repair_orders = root.findall(".//ns:RepairOrder", namespaces=ns)
    for repair_order in repair_orders:
        db_dealer_integration_partner = {
            "dms_id": dealer_number
        }
        db_service_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        ro_record = repair_order.find(".//ns:RoRecord", namespaces=ns)
        if ro_record is not None:
            rogen = ro_record.find(".//ns:Rogen", namespaces=ns)
            if rogen is not None:
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

                ro_comment_infos = rogen.findall(".//ns:RoCommentInfo", namespaces=ns)
                for ro_comment_info in ro_comment_infos:
                    comment = ro_comment_info.get("RoComment")
                    if comment is not None:
                        db_service_repair_order.setdefault("comment", []).append(comment)

                tech_recommends = rogen.findall(".//ns:TechRecommends", namespaces=ns)
                for tech_recommend in tech_recommends:
                    recommendation = tech_recommend.get("TechRecommend")
                    if recommendation is not None:
                        db_service_repair_order.setdefault("recommendation", []).append(recommendation)

            ro_labor = ro_record.find(".//ns:Rolabor", namespaces=ns)
            if ro_labor is not None:
                ro_amounts = ro_labor.findall(".//ns:RoAmts", namespaces=ns)
                txn_pay_type_arr = set()
                for ro_amount in ro_amounts:
                    pay_type = ro_amount.get("PayType")
                    txn_pay_type_arr.add(pay_type)
                db_service_repair_order["txn_pay_type"] = ",".join(list(txn_pay_type_arr))

                op_code_labor_infos = ro_labor.findall(".//ns:OpCodeLaborInfo", namespaces=ns)
                for op_code_labor_info in op_code_labor_infos:
                    db_op_code = {}
                    db_op_code["op_code|op_code"] = op_code_labor_info.get("OpCode")
                    db_op_code["op_code|op_code_desc"] = op_code_labor_info.get("OpCodeDesc")
                    db_op_codes.append(db_op_code)

        service_vehicle = repair_order.find(".//ns:ServVehicle", namespaces=ns)
        if service_vehicle is not None:
            vehicle_service_info = service_vehicle.find(".//ns:VehicleServInfo", namespaces=ns)
            if vehicle_service_info is not None:
                db_service_repair_order["ro_close_date"] = vehicle_service_info.get("LastRODate")
                db_vehicle["stock_num"] = vehicle_service_info.get("StockID")
            rr_vehicle = service_vehicle.find(".//ns:Vehicle", namespaces=ns)
            if rr_vehicle is not None:
                db_vehicle["make"] = rr_vehicle.get("VehicleMake")
                db_vehicle["model"] = rr_vehicle.get("Carline")
                db_vehicle["year"] = rr_vehicle.get("VehicleYr")

        customer_record = repair_order.find(".//ns:CustRecord", namespaces=ns)
        if customer_record is not None:
            contact_info = customer_record.find(".//ns:ContactInfo", namespaces=ns)
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

                email = contact_info.find(".//ns:Email", namespaces=ns)
                if email is not None:
                    mail_to = email.get("MailTo")
                    db_consumer["email"] = mail_to
                    email_optin_flag = isinstance(mail_to, str) and "@" in mail_to
                    db_consumer["email_optin_flag"] = email_optin_flag

                address = contact_info.find(".//ns:Address", namespaces=ns)
                if address is not None:
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
