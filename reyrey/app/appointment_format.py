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

    #How do component and task come into play?
    dealer_number = None
    store_number = None
    area_number = None
    if application_area is not None:
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
        "s3_url": s3_uri,
    }

    service_appointments = root.findall(".//ns:ServAppointment", namespaces=ns)
    for service_appointment in service_appointments:
        db_dealer_integration_partner = {"dms_id": dms_id}
        db_service_appointment = {}
        db_vehicle = {}
        db_consumer = {}
        db_service_contracts = []

        db_service_appointment["appointment_no"] = service_appointment.get("ApptNo")

        if service_appointment.get("ApptStatus") == "Update" or service_appointment.get("ApptStatus") == "Delete":
            db_service_appointment["rescheduled_flag"] = True
        else:
            db_service_appointment["rescheduled_flag"] = False

        if application_area is not None:
            db_service_appointment["appointment_create_ts"] = application_area.find(".//ns:CreationDateTime", namespaces=ns).text

        appointment_time = service_appointment.find(".//ns:AppointmentTime", namespaces=ns)
        if appointment_time is not None:
            date_time_stamp = appointment_time.find(".//ns:DateTimeStamp", namespaces=ns)
            if date_time_stamp is not None:
                db_service_appointment["appointment_time"] = date_time_stamp.get("Time")
                db_service_appointment["appointment_date"] = date_time_stamp.get("Date")

        customer_record = service_appointment.find(".//ns:CustRecord", namespaces=ns)
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

        service_vehicle = service_appointment.find(".//ns:ServVehicle", namespaces=ns)
        if service_vehicle is not None:
            vehicle_service_info = service_vehicle.find(
                ".//ns:VehicleServInfo", namespaces=ns
            )
            if vehicle_service_info is not None:
                db_service_appointment["last_ro_date"] = vehicle_service_info.get(
                    "LastRODate"
                )
                db_service_appointment["last_ro_num"] = vehicle_service_info.get(
                    "LastRONo"
                )
                db_vehicle["stock_num"] = vehicle_service_info.get("StockID")
                db_vehicle["warranty_expiration_miles"] = vehicle_service_info.get(
                    "FactoryWarrExpOdom"
                )
                db_vehicle["warranty_expiration_date"] = vehicle_service_info.get(
                    "FactoryWarrExpDate"
                )
                for vehicle_ext_warranty in vehicle_service_info.findall(
                    ".//ns:VehExtWarranty", namespaces=ns
                ):
                    db_service_contract = {}
                    db_service_contract["service_contracts|contract_id"] = vehicle_ext_warranty.get(
                        "ContractNo"
                    )
                    db_service_contract["service_contracts|contract_name"] = vehicle_ext_warranty.get(
                        "ContractName"
                    )
                    db_service_contract["service_contracts|expiration_miles"] = vehicle_ext_warranty.get(
                        "ExpirationOdom"
                    )
                    db_service_contract["service_contracts|warranty_expiration_date"] = vehicle_ext_warranty.get(
                        "ExpirationDate"
                    )
                    if db_service_contract and any(value is not None for value in db_service_contract.values()):
                        db_service_contracts.append(db_service_contract)

            rr_vehicle = service_vehicle.find(".//ns:Vehicle", namespaces=ns)
            if rr_vehicle is not None:
                db_vehicle["vin"] = rr_vehicle.get("Vin")
                db_vehicle["make"] = rr_vehicle.get("VehicleMake")
                db_vehicle["model"] = rr_vehicle.get("Carline")
                db_vehicle["year"] = rr_vehicle.get("VehicleYr")
                vehicle_detail = rr_vehicle.find(".//ns:VehicleDetail", namespaces=ns)
                if vehicle_detail is not None:
                    db_vehicle["mileage"] = vehicle_detail.get("OdomReading")

            outside_appt_src = service_appointment.find(".//ns:OutsideApptSrc", namespaces=ns)
            if outside_appt_src.get("Appointment") is not None:
                db_service_appointment["appointment_source"] = 'External'

        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_service_appointment["metadata"] = metadata

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "appointment": db_service_appointment,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "service_contracts.service_contracts": db_service_contracts,
        }
        entries.append(entry)
    return entries, dms_id

def lambda_handler(event, context):
    """Transform reyrey service appointment files."""
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
                upload_unified_json(entries, "service_appointment", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming reyrey appointment file {event}")
        raise
