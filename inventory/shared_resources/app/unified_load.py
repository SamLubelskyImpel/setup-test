import os
import json
import logging
import urllib.parse
import boto3
from rds_instance import RDSInstance
from json import loads
from io import BytesIO

ENVIRONMENT = os.environ['ENVIRONMENT']

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')


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
        'on_lot': json_data.get('inv_inventory|on_lot', True),
        'metadata': extract_field('inv_inventory|metadata'),
        'received_datetime': extract_field('inv_inventory|received_datetime'),
        'vdp': extract_field('inv_inventory|vdp'),
        'comments': extract_field('inv_inventory|comments'),
    }
    return inventory_data


def extract_option_data(option_json):
    option_data = {
        'option_description': option_json.get('inv_option|option_description', ''),
        'is_priority': option_json.get('inv_option|is_priority', False)
    }
    return option_data


def process_and_upload_data(bucket, key, rds_instance: RDSInstance):
    try:
        decoded_key = urllib.parse.unquote_plus(key)
        s3_obj = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        json_data_list = json.load(BytesIO(s3_obj["Body"].read()))
        #  Check if incoming data is older than existing data
        sample_data = json_data_list[0] if json_data_list else None
        if sample_data:
            incoming_received_datetime = sample_data.get('inv_inventory|received_datetime')
            provider_dealer_id = sample_data.get("inv_dealer_integration_partner|provider_dealer_id")
            if provider_dealer_id == '215441':
                logger.warning(f"Temporarily Skipping data processing for provider dealer ID: {provider_dealer_id} (Flexicar)")
                return

            if incoming_received_datetime and not rds_instance.is_new_data(incoming_received_datetime, provider_dealer_id):
                logger.warning("Incoming data is older than existing data. Processing stopped.")
                return

        # Retrieve dealer_integration_partner_id
        provider_dealer_id = sample_data.get("inv_dealer_integration_partner|provider_dealer_id")
        if provider_dealer_id is None:
            logger.warning(f"No provider dealer ID found for {decoded_key}, skipping.")
            return

        dealer_integration_partner_id = rds_instance.find_dealer_integration_partner_id(provider_dealer_id)
        if dealer_integration_partner_id is None:
            logger.warning(f"No dealer integration partner ID found for provider dealer ID: {provider_dealer_id}, skipping.")
            return

        processed_inventory_ids = []
        for json_data in json_data_list:
            # Insert vehicle data
            vehicle_data = extract_vehicle_data(json_data)
            vehicle_data['dealer_integration_partner_id'] = dealer_integration_partner_id
            vehicle_id = rds_instance.insert_vehicle(vehicle_data)

            # Insert inventory data
            inventory_data = extract_inventory_data(json_data)
            inventory_data['vehicle_id'] = vehicle_id
            inventory_data['dealer_integration_partner_id'] = dealer_integration_partner_id
            inventory_id = rds_instance.insert_inventory_item(inventory_data)
            processed_inventory_ids.append(inventory_id)

            # Insert options data
            if json_data.get('inv_options|inv_options'):
                option_ids = []
                for option_json in json_data['inv_options|inv_options']:
                    option_data = extract_option_data(option_json)
                    option_id = rds_instance.insert_option(option_data)
                    option_ids.append(option_id)
                rds_instance.link_option_to_inventory(inventory_id, option_ids)
            else:
                identifier = json_data.get(json_data.get('inv_vehicle|stock_num', 'Unknown Identifier'))
                logger.warning(f"Skipping options for record with identifier {identifier} as they do not exist.")

        #  Use dealer_integration_partner_id to update the value of on_lot for vehicles
        dealer_id = dealer_integration_partner_id
        rds_instance.update_dealers_other_vehicles(dealer_id, processed_inventory_ids)
        logger.info(f"Data processing and upload completed for {decoded_key}")

    except Exception:
        logger.exception(f"Failed to process and upload data for {key}", exc_info=True)
        raise


def lambda_handler(event, _):
    """
    Lambda function handler to process SQS messages and upload JSON data to RDS.
    """
    try:
        rds_instance = RDSInstance()
        count = 0
        for record in event['Records']:
            logger.info(f"Processing record {count}")
            message = loads(record["body"])
            logger.info(f"Received message: {message}")

            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                process_and_upload_data(bucket, key, rds_instance)
            count += 1
    except Exception:
        logger.exception("Error in Lambda handler")
        raise
