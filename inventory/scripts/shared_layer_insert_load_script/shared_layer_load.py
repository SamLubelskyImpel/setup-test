import os
import json
import logging
from typing import Union
import urllib.parse
import boto3
from rds_instance import RDSInstance
from json import loads
from io import BytesIO

ENVIRONMENT = os.environ.get("ENVIRONMENT", "test")
BUCKET_NAME = (
    "inventory-integrations-us-east-1-prod"
    if ENVIRONMENT == "prod"
    else "inventory-integrations-us-east-1-test"
)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")

# Define the folder path
folder_path = "inserted_inventory_records"

# Ensure the folder exists
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Check if the logger already has handlers
if not logger.handlers:
    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a formatter and set it for the handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)


def extract_vehicle_data(json_data):
    # Attempt to get the year and convert it to an integer if possible
    #  There was an entry whose year was an empty string, so we need to handle this case
    year_value = json_data.get("inv_vehicle|year")
    try:
        # Only attempt the conversion if year_value is not an empty string
        year = int(year_value) if year_value.strip() else None
    except (ValueError, TypeError, AttributeError):
        # Set to None if conversion fails or if year_value is None
        year = None

    new_or_used_raw = json_data.get("inv_vehicle|new_or_used", "").strip().lower()
    if new_or_used_raw == "new":
        new_or_used = "N"
    elif new_or_used_raw == "used":
        new_or_used = "U"
    else:
        new_or_used = ""

    vehicle_data = {
        "vin": json_data.get("inv_vehicle|vin", ""),
        "oem_name": json_data.get("inv_vehicle|oem_name", ""),
        "type": json_data.get("inv_vehicle|type", ""),
        "mileage": json_data.get("inv_vehicle|mileage", ""),
        "make": json_data.get("inv_vehicle|make", ""),
        "model": json_data.get("inv_vehicle|model", ""),
        "year": year,
        "stock_num": json_data.get("inv_vehicle|stock_num", ""),
        "new_or_used": new_or_used,
    }
    return vehicle_data


def extract_inventory_data(json_data):
    def extract_field(field, ftype="str"):
        value = json_data.get(field)
        if not value:
            return None

        # Remove spaces from the value for inv_inventory|cylinders
        if field == "inv_inventory|cylinders":
            value = value.strip().replace(" ", "")

        if ftype == "int":
            return int(value.strip())
        elif ftype == "float":
            return float(value.strip())
        elif ftype == "str":
            return value.strip()
        else:
            logger.error(f"Invalid field: {ftype}")
            raise ValueError(f"Invalid field type: {ftype}")

    inventory_data = {
        "list_price": extract_field("inv_inventory|list_price", ftype="float"),
        "special_price": extract_field("inv_inventory|special_price", ftype="float"),
        "fuel_type": extract_field("inv_inventory|fuel_type"),
        "exterior_color": extract_field("inv_inventory|exterior_color"),
        "interior_color": extract_field("inv_inventory|interior_color"),
        "doors": extract_field("inv_inventory|doors", ftype="int"),
        "seats": extract_field("inv_inventory|seats", ftype="int"),
        "transmission": extract_field("inv_inventory|transmission"),
        "photo_url": extract_field("inv_inventory|photo_url"),
        "drive_train": extract_field("inv_inventory|drive_train"),
        "cylinders": extract_field("inv_inventory|cylinders", ftype="int"),
        "body_style": extract_field("inv_inventory|body_style"),
        "series": extract_field("inv_inventory|series"),
        "vin": extract_field("inv_inventory|vin"),
        "interior_material": extract_field("inv_inventory|interior_material"),
        "trim": extract_field("inv_inventory|trim"),
        "factory_certified": json_data.get("inv_inventory|factory_certified", False),
        "region": extract_field("inv_inventory|region"),
        "on_lot": json_data.get("inv_inventory|on_lot", True),
        "metadata": extract_field("inv_inventory|metadata"),
        "received_datetime": extract_field("inv_inventory|received_datetime"),
        "vdp": extract_field("inv_inventory|vdp"),
        "comments": extract_field("inv_inventory|comments"),
    }
    return inventory_data


def extract_option_data(option_json):
    option_data = {
        "option_description": option_json.get("inv_option|option_description", ""),
        "is_priority": option_json.get("inv_option|is_priority", False),
    }
    return option_data


def process_and_upload_data(bucket, key, rds_instance: RDSInstance):
    try:
        decoded_key = urllib.parse.unquote_plus(key)
        logger.info(f"Processing and uploading data for {decoded_key}")
        s3_obj = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        json_data_list = json.load(BytesIO(s3_obj["Body"].read()))

        sample_data = json_data_list[0] if json_data_list else None

        # Retrieve dealer_integration_partner_id
        provider_dealer_id = sample_data.get(
            "inv_dealer_integration_partner|provider_dealer_id"
        )
        if provider_dealer_id is None:
            logger.warning(f"No provider dealer ID found for {decoded_key}, skipping.")
            return

        dealer_integration_partner_id = rds_instance.find_dealer_integration_partner_id(
            provider_dealer_id
        )
        if dealer_integration_partner_id is None:
            logger.warning(
                "No dealer integration partner ID found for provider dealer ID: %s, skipping.",
                provider_dealer_id,
            )
            return

        processed_inventory_ids = []
        inserted_inventory_map: dict[int, int] = {}

        for json_data in json_data_list:
            # Insert vehicle data
            vehicle_data = extract_vehicle_data(json_data)
            vehicle_data["dealer_integration_partner_id"] = (
                dealer_integration_partner_id
            )
            vehicle_id = rds_instance.insert_vehicle(vehicle_data)

            if vehicle_id is None:
                logger.warning(
                    "Skipping record with stock number %s as it already exists.",
                    vehicle_data['stock_num']
                )
                continue
            else:
                logger.info(
                    "Inserted vehicle record with stock number %s and vehicle ID %d",
                    vehicle_data["stock_num"],
                    vehicle_id,
                )

            # Insert inventory data
            inventory_data = extract_inventory_data(json_data)
            inventory_data["vehicle_id"] = vehicle_id
            inventory_data["dealer_integration_partner_id"] = (
                dealer_integration_partner_id
            )

            # Set on_lot to False for all data inserted data
            inventory_data["on_lot"] = False
            inventory_id = rds_instance.insert_inventory_item(inventory_data)
            processed_inventory_ids.append(inventory_id)

            # Store the inventory_id and vehicle_id in the map
            inserted_inventory_map[inventory_id] = vehicle_id

            # Insert options data
            if json_data.get("inv_options|inv_options"):
                option_data_list = []
                for option_json in json_data["inv_options|inv_options"]:
                    option_data = extract_option_data(option_json)
                    option_data_list.append(option_data)

                # Perform bulk insert
                option_ids = rds_instance.insert_options(option_data_list)

                # Link the inserted options to the inventory
                rds_instance.link_option_to_inventory(inventory_id, option_ids)
            else:
                identifier = json_data.get(
                    json_data.get("inv_vehicle|stock_num", "Unknown Identifier")
                )

                logger.warning(
                    f"Skipping options for record with identifier {identifier} as they do not exist."
                )

        logger.info(
            "Data processing completed. Total records processed: %d",
            len(json_data_list),
        )

        # Save the inventory_options_map
        file_name = f"{decoded_key.split('.')[0]}_inventory_options_map.json"
        file_path = os.path.join(folder_path, file_name)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as file:
            json.dump(inserted_inventory_map, file)

        logger.info(f"Inventory and options data saved to {file_path}")
        logger.info(f"Data processing and upload completed for {decoded_key}")

    except Exception:
        logger.exception(f"Failed to process and upload data for {key}", exc_info=True)
        raise


def list_s3_keys(prefix, bucket_name=BUCKET_NAME):
    keys = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )

        if "Contents" in response:
            for obj in response["Contents"]:
                keys.append(obj["Key"])

        if response.get("IsTruncated"):  # Check if there are more keys to fetch
            continuation_token = response.get("NextContinuationToken")
        else:
            break

    return keys


# This script loads and inserts inventory data into the database for a given date and partner
if __name__ == "__main__":
    try:
        KEY_PREFIX_DATE = "unified/coxau/2024/10/24"
        logger.info("Starting script execution")

        reversed_keys = list_s3_keys(KEY_PREFIX_DATE)[::-1]

        for key in reversed_keys:
            logger.info(f"Processing key: {key}")
            process_and_upload_data(BUCKET_NAME, key, RDSInstance())

    except Exception:
        logger.exception("Error in script execution")
