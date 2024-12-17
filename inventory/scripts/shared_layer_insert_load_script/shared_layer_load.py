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


def save_ids_to_file(vehicle_ids, invetory_ids, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        json.dump({"vehicle_ids": vehicle_ids, "inventory_ids": invetory_ids}, f)


def extract_vehicle_data(json_data):
    # Attempt to get the year and convert it to an integer if possible
    #  There was an entry whose year was an empty string, so we need to handle this case
    year_value = json_data.get('inv_vehicle|year')
    try:
        # Only attempt the conversion if year_value is not an empty string
        year = int(year_value) if year_value.strip() else None
    except (ValueError, TypeError, AttributeError):
        # Set to None if conversion fails or if year_value is None
        year = None

    new_or_used_raw = json_data.get('inv_vehicle|new_or_used', '').strip().lower()
    if new_or_used_raw == 'new':
        new_or_used = 'N'
    elif new_or_used_raw == 'used':
        new_or_used = 'U'
    else:
        new_or_used = ''

    vehicle_data = {
        'vin': json_data.get('inv_vehicle|vin', ''),
        'oem_name': json_data.get('inv_vehicle|oem_name', ''),
        'type': json_data.get('inv_vehicle|type', ''),
        'mileage': json_data.get('inv_vehicle|mileage', ''),
        'make': json_data.get('inv_vehicle|make', ''),
        'model': json_data.get('inv_vehicle|model', ''),
        'year': year,
        'stock_num': json_data.get('inv_vehicle|stock_num', ''),
        'new_or_used': new_or_used,
    }
    return vehicle_data


def extract_inventory_data(json_data):
    def extract_field(field, ftype='str'):
        value = json_data.get(field)
        if not value:
            return None

        # Remove spaces from the value for inv_inventory|cylinders
        if field == 'inv_inventory|cylinders':
            value = value.strip().replace(' ', '')

        if ftype == 'int':
            return int(value.strip())
        elif ftype == 'float':
            return float(value.strip())
        elif ftype == 'str':
            return value.strip()
        elif ftype == 'list':
            return value
        else:
            logger.error(f"Invalid field: {ftype}")
            raise ValueError(f"Invalid field type: {ftype}")

    inventory_data = {
        'list_price': extract_field('inv_inventory|list_price', ftype='float'),
        'special_price': extract_field('inv_inventory|special_price', ftype='float'),
        'fuel_type': extract_field('inv_inventory|fuel_type'),
        'exterior_color': extract_field('inv_inventory|exterior_color'),
        'interior_color': extract_field('inv_inventory|interior_color'),
        'doors': extract_field('inv_inventory|doors', ftype='int'),
        'seats': extract_field('inv_inventory|seats', ftype='int'),
        'transmission': extract_field('inv_inventory|transmission'),
        'photo_url': extract_field('inv_inventory|photo_url'),
        'drive_train': extract_field('inv_inventory|drive_train'),
        'cylinders': extract_field('inv_inventory|cylinders', ftype='int'),
        'body_style': extract_field('inv_inventory|body_style'),
        'series': extract_field('inv_inventory|series'),
        'vin': extract_field('inv_inventory|vin'),
        'interior_material': extract_field('inv_inventory|interior_material'),
        'trim': extract_field('inv_inventory|trim'),
        'factory_certified': json_data.get('inv_inventory|factory_certified', False),
        'region': extract_field('inv_inventory|region'),
        'on_lot': json_data.get('inv_inventory|on_lot', False),
        'metadata': extract_field('inv_inventory|metadata'),
        'received_datetime': extract_field('inv_inventory|received_datetime'),
        'vdp': extract_field('inv_inventory|vdp'),
        'comments': extract_field('inv_inventory|comments'),
        'options': extract_field('inv_inventory|options', ftype='list'),
        'priority_options': extract_field('inv_inventory|priority_options', ftype='list'),
        "vehicle_data": {  # Fields used to match vehicle data
            'vin': json_data.get('inv_vehicle|vin', ''),
            'model': json_data.get('inv_vehicle|model', ''),
            'stock_num': json_data.get('inv_vehicle|stock_num', ''),
            'mileage': json_data.get('inv_vehicle|mileage', '')
        }
    }
    return inventory_data


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

        # Accumulate records for batch processing
        vehicle_data_list = []
        inventory_data_list = []

        logger.info("Starting data processing.")
        for json_data in json_data_list:
            # Extract vehicle data
            vehicle_data = extract_vehicle_data(json_data)
            vehicle_data_list.append(vehicle_data)

            # Extract inventory data
            inventory_data = extract_inventory_data(json_data)
            inventory_data_list.append(inventory_data)

        # Perform batch upsert for vehicles
        vehicle_id_map, new_vehicle_ids = rds_instance.batch_upsert_vehicles(vehicle_data_list, dealer_integration_partner_id)

        logger.info(f"Processed vehicles {len(vehicle_id_map)}")

        # Use `vehicle_id_map` to set `vehicle_id` in inventory records before batch processing inventory
        for inventory_data in inventory_data_list:
            # Assuming VIN or other key is used to look up the vehicle ID from vehicle_id_map
            vehicle_key = (
                    inventory_data['vehicle_data'].get('vin'),
                    dealer_integration_partner_id,
                    inventory_data['vehicle_data'].get('model'),
                    inventory_data['vehicle_data'].get('stock_num'),
                    inventory_data['vehicle_data'].get('mileage')
                )
            inventory_data['vehicle_id'] = vehicle_id_map.get(vehicle_key)
            if not inventory_data['vehicle_id']:
                logger.warning(f"Vehicle ID not found for inventory record with key {vehicle_key}")
                raise

        # Perform batch insert or update for inventory items
        processed_inventory_ids = rds_instance.batch_insert_inventory(inventory_data_list, dealer_integration_partner_id)

        # Save the vehicle and inventory IDs to a file
        save_ids_to_file(new_vehicle_ids, list(processed_inventory_ids), f"{folder_path}/{decoded_key}")

        logger.info("Data processing completed. Total records processed: %d",
                    len(json_data_list))

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
        # Define date
        DATE = "2024/10/25"

        integration_partners = [
            'dealerstudio',
            'icc',
            'coxau'
        ]

        for integration_partner in integration_partners:
            KEY_PREFIX_DATE = f"unified/{integration_partner}/{DATE}"
            reversed_keys = list_s3_keys(KEY_PREFIX_DATE)[::-1]

            for key in reversed_keys:
                logger.info(f"Processing key: {key}")
                process_and_upload_data(BUCKET_NAME, key, RDSInstance())
    except Exception:
        logger.exception("Error in script execution")
