"""Format tekion json data to unified format."""
import logging
import urllib.parse
from datetime import datetime
from json import dumps, loads
from os import environ
import boto3
from unified_df import upload_unified_json

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
    return datetime.utcfromtimestamp(unix_time / 1000).strftime("%Y-%m-%d %H:%M:%S")


def parse_json_to_entries(json_data, s3_uri):
    """Format tekion json data to unified format."""
    entries = []

    dms_id = None
    for repair_order in json_data:
        db_dealer_integration_partner = {}
        db_service_repair_order = {}
        db_vehicle = {}
        db_consumer = {}
        db_op_codes = []

        db_metadata = {
            "Region": REGION,
            "PartitionYear": s3_uri.split("/")[2],
            "PartitionMonth": s3_uri.split("/")[3],
            "PartitionDate": s3_uri.split("/")[4],
            "s3_url": s3_uri,
        }

        dms_id = default_get(repair_order, "dms_id")  # Added to payload from parent lambda
        db_dealer_integration_partner["dms_id"] = dms_id

        db_service_repair_order["repair_order_no"] = default_get(
            repair_order, "repairOrderNumber"
        )
        created_time = default_get(repair_order, "checkinTime")
        db_service_repair_order["ro_open_date"] = convert_unix_to_timestamp(
            created_time
        )
        closed_time = default_get(repair_order, "closedTime")
        db_service_repair_order["ro_close_date"] = convert_unix_to_timestamp(
            closed_time
        )

        primary_advisor = default_get(repair_order, "primaryAdvisor", [])
        for advisor in primary_advisor:
            first_name = default_get(advisor, "firstName")
            last_name = default_get(advisor, "lastName")
            if first_name or last_name:
                db_service_repair_order["advisor_name"] = f"{first_name} {last_name}"

        invoice = default_get(repair_order, "invoice", {})
        db_service_repair_order["total_amount"] = default_get(invoice, "invoiceAmount")

        customer_pay = default_get(invoice, "customerPay", {})
        db_service_repair_order["consumer_total_amount"] = default_get(customer_pay, "amount")

        warranty_pay = default_get(invoice, "warrantyPay", {})
        db_service_repair_order["warranty_total_amount"] = default_get(warranty_pay, "amount")

        internal_pay = default_get(invoice, "internalPay", {})
        db_service_repair_order["internal_total_amount"] = default_get(internal_pay, "amount")  

        consumer_labor_amount = default_get(invoice, "customerPay", {})
        db_service_repair_order["consumer_labor_amount"] = default_get(consumer_labor_amount, "laborAmount")  

        consumer_parts_amount = default_get(invoice, "customerPay", {})
        db_service_repair_order["consumer_parts_amount"] = default_get(consumer_parts_amount, "partsAmount")  

        customer_pay_parts_cost = default_get(invoice, "customerPay", {})
        db_service_repair_order["customer_pay_parts_cost"] = default_get(customer_pay_parts_cost, "partCostAmount")  

        warranty_labor_amount = default_get(invoice, "warrantyPay", {})
        db_service_repair_order["warranty_labor_amount"] = default_get(warranty_labor_amount, "laborAmount")  

        warranty_parts_cost = default_get(invoice, "warrantyPay", {})
        db_service_repair_order["warranty_parts_cost"] = default_get(warranty_parts_cost, "partCostAmount")  

        internal_parts_cost = default_get(invoice, "internalPay", {})
        db_service_repair_order["internal_parts_cost"] = default_get(internal_parts_cost, "partCostAmount")  

        total_internal_labor_amount = default_get(invoice, "internalPay", {})
        db_service_repair_order["total_internal_labor_amount"] = default_get(total_internal_labor_amount, "laborAmount")  

        total_internal_parts_amount = default_get(invoice, "internalPay", {})
        db_service_repair_order["total_internal_parts_amount"] = default_get(total_internal_parts_amount, "partsAmount")  

        db_service_repair_order["service_order_status"] = default_get(repair_order, "status")

        insurance_pay = default_get(invoice, "insurancePay", {})

        # Service Cost
        service_cost_fields = []
        service_cost_fields.append(default_get(customer_pay, "laborCostAmount"))
        service_cost_fields.append(default_get(customer_pay, "partCostAmount"))

        service_cost_fields.append(default_get(warranty_pay, "laborCostAmount"))
        service_cost_fields.append(default_get(warranty_pay, "partCostAmount"))

        service_cost_fields.append(default_get(internal_pay, "laborCostAmount"))
        service_cost_fields.append(default_get(internal_pay, "partCostAmount"))

        service_cost_fields.append(default_get(insurance_pay, "laborCostAmount"))
        service_cost_fields.append(default_get(insurance_pay, "partCostAmount"))

        db_service_repair_order["service_order_cost"] = str(sum(float(i) if i is not None else 0 for i in service_cost_fields))

        txn_pay_type_arr = set()
        comment = set()
        jobs = default_get(repair_order, "jobs", [])
        for job in jobs:
            pay_type = default_get(job, "payType")
            if pay_type:
                txn_pay_type_arr.add(pay_type)
            concern = default_get(job, "concern")
            if concern:
                comment.add(concern)
            operations = default_get(job, "operations", [])
            for operation in operations:
                db_op_code = {}
                db_op_code["op_code|op_code"] = default_get(operation, "opcode")
                db_op_code["op_code|op_code_desc"] = (default_get(operation, "opcodeDescription") or "")[:305]
                db_op_codes.append(db_op_code)

        db_service_repair_order["txn_pay_type"] = ",".join(list(txn_pay_type_arr))
        db_service_repair_order["comment"] = ",".join(list(txn_pay_type_arr))

        vehicle = default_get(repair_order, "vehicle", {})
        db_vehicle["vin"] = default_get(vehicle, "vin")
        db_vehicle["oem_name"] = default_get(vehicle, "make")
        db_vehicle["make"] = default_get(vehicle, "make")
        db_vehicle["model"] = default_get(vehicle, "model")
        db_vehicle["year"] = default_get(vehicle, "year")
        mileage_in = default_get(vehicle, "mileageOut")
        if default_get(mileage_in, "unit", "").upper() == "MI":
            db_vehicle["mileage"] = default_get(mileage_in, "value")

        customer = default_get(repair_order, "customer", {})
        db_consumer["dealer_customer_no"] = default_get(customer, "arcId")
        db_consumer["first_name"] = default_get(customer, "firstName")
        db_consumer["last_name"] = default_get(customer, "lastName")
        db_consumer["email"] = default_get(customer, "email")
        phones = default_get(customer, "phones", [])
        for phone in phones:
            phone_type = default_get(phone, "phoneType", "")
            phone_number = default_get(phone, "number")
            if phone_type.upper() == "MOBILE":
                db_consumer["cell_phone"] = phone_number
            elif phone_type.upper() == "HOME":
                db_consumer["home_phone"] = phone_number
        address = default_get(customer, "address", {})
        db_consumer["city"] = default_get(address, "city")
        db_consumer["state"] = default_get(address, "state")
        db_consumer["postal_code"] = default_get(address, "zip")
        address_line1 = default_get(address, "line1")
        address_line2 = default_get(address, "line2")
        if address_line1 and address_line2:
            db_consumer["address"] = f"{address_line1} {address_line2}"
        elif address_line1:
            db_consumer["address"] = address_line1

        metadata = dumps(db_metadata)
        db_vehicle["metadata"] = metadata
        db_consumer["metadata"] = metadata
        db_service_repair_order["metadata"] = metadata

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "service_repair_order": db_service_repair_order,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "op_codes.op_codes": db_op_codes
        }
        entries.append(entry)
    return entries, dms_id


def lambda_handler(event, context):
    """Transform tekion repair order files."""
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
                json_data = loads(response["Body"].read())
                entries, dms_id = parse_json_to_entries(json_data, decoded_key)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "repair_order", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion repair order file {event}")
        raise
