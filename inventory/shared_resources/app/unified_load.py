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
        if isinstance(year_value, str):
            year = int(year_value) if year_value.strip() else None
        else:
            year = year_value
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
        'metadata': json_data.get('inv_vehicle|metadata'),
        'vehicle_class': json_data.get('inv_vehicle|vehicle_class', '')
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
            return value if isinstance(value, int) else int(value.strip())
        elif ftype == 'float':
            return value if isinstance(value, float) else float(value.strip())
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
        'on_lot': json_data.get('inv_inventory|on_lot', True),
        'metadata': extract_field('inv_inventory|metadata'),
        'received_datetime': extract_field('inv_inventory|received_datetime'),
        'vdp': extract_field('inv_inventory|vdp'),
        'comments': extract_field('inv_inventory|comments'),
        'options': extract_field('inv_inventory|options', ftype='list'),
        'priority_options': extract_field('inv_inventory|priority_options', ftype='list'),
        'cost_price': extract_field('inv_inventory|cost_price', ftype='float'),
        'inventory_status': extract_field('inv_inventory|inventory_status'),
        'source_data_drive_train': extract_field('inv_inventory|source_data_drive_train'),
        'source_data_interior_material_description': extract_field('inv_inventory|source_data_interior_material_description'),
        'source_data_transmission': extract_field('inv_inventory|source_data_transmission'),
        'source_data_transmission_speed': extract_field('inv_inventory|source_data_transmission_speed'),
        'transmission_speed': extract_field('inv_inventory|transmission_speed'),
        'build_data': extract_field('inv_inventory|build_data'),
        'highway_mpg': extract_field('inv_inventory|highway_mpg', ftype='int'),
        'city_mpg': extract_field('inv_inventory|city_mpg', ftype='int'),
        'engine': extract_field('inv_inventory|engine'),
        'engine_displacement': extract_field('inv_inventory|engine_displacement'),
        'vehicle_data': {  # Fields used to match vehicle data
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
        s3_obj = s3_client.get_object(Bucket=bucket, Key=decoded_key)
        json_data_list = json.load(BytesIO(s3_obj["Body"].read()))
        # Check if incoming data is older than existing data
        sample_data = json_data_list[0] if json_data_list else None
        if sample_data:
            incoming_received_datetime = sample_data.get('inv_inventory|received_datetime')
            provider_dealer_id = sample_data.get("inv_dealer_integration_partner|provider_dealer_id")

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
        vehicle_id_map = rds_instance.batch_upsert_vehicles(vehicle_data_list, dealer_integration_partner_id)

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

        logger.info("Data processing completed. Total records processed: %d",
                    len(json_data_list))

        # Use dealer_integration_partner_id to update the value of on_lot for vehicles
        partner = key.split('/')[1]
        if partner not in ['carsales']:
            rds_instance.update_dealers_other_vehicles(dealer_integration_partner_id, processed_inventory_ids)
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
        for record in event['Records']:
            message = loads(record["body"])
            logger.info(f"Received message: {message}")

            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                process_and_upload_data(bucket, key, rds_instance)
    except Exception:
        logger.exception("Error in Lambda handler")
        raise
