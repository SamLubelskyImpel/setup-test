import boto3
from json import loads
from os import environ
from typing import Any
import logging
import pandas as pd
from datetime import datetime
import tempfile
import paramiko
from io import BytesIO
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from rds_instance import RDSInstance

ENVIRONMENT = environ["ENVIRONMENT"]
INVENTORY_BUCKET = environ["INVENTORY_BUCKET"]
MERCH_SFTP_KEY = environ["MERCH_SFTP_KEY"]
SALESAI_SFTP_KEY = environ["SALESAI_SFTP_KEY"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
sm_client = boto3.client("secretsmanager")


def proccess_and_upload_to_sftp(icc_formatted_inventory, product_dealer_id, secret_key) -> None:
    """Upload to ftp server."""
    # Set DealerId to match product expected DealerId
    icc_formatted_inventory["DealerId"] = product_dealer_id

    csv_content = icc_formatted_inventory.to_csv(index=False)

    # Upload to SFTP
    hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", secret_key)
    prefix = '' if ENVIRONMENT == 'prod' else 'deleteme_'
    filename = f"{prefix}{product_dealer_id}.csv"

    with connect_sftp_server(hostname, port, username, password) as sftp:
        csv_file_like = BytesIO(csv_content.encode())
        sftp.putfo(csv_file_like, filename)

    logger.info(f"File {filename} uploaded to SFTP")


def connect_sftp_server(hostname, port, username, password):
    """Connect to SFTP server and return the connection."""
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


def upload_to_s3(csv_content, filename, integration):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"icc/{integration}/{date_key}/{filename}"
    s3_client.put_object(
        Bucket=INVENTORY_BUCKET,
        Key=s3_key,
        Body=csv_content
    )
    logger.info(f"File {s3_key} uploaded to S3")


def convert_unified_to_icc(unified_inventory: list) -> pd.DataFrame:
    """Convert unified inventory to ICC format."""
    field_mappings = {
        "DealerId": "inv_dealer_integration_partner|provider_dealer_id",

        "VIN": "inv_vehicle|vin",
        "BodyType": "inv_vehicle|type",
        "Mileage": "inv_vehicle|mileage",
        "Make": "inv_vehicle|make",
        "Model": "inv_vehicle|model",
        "Year": "inv_vehicle|year",
        "isNew": "inv_vehicle|new_or_used",
        "Stock": "inv_vehicle|stock_num",

        "ListPrice": "inv_inventory|list_price",
        "CostPrice": "inv_inventory|cost_price",
        "FuelType": "inv_inventory|fuel_type",
        "Exteriorcolor": "inv_inventory|exterior_color",
        "Interiorcolor": "inv_inventory|interior_color",
        "DoorCount": "inv_inventory|doors",
        "Transmission": "inv_inventory|transmission",
        "PhotoUrl": "inv_inventory|photo_url",
        "DealerComments": "inv_inventory|comments",
        "Drivetrain": "inv_inventory|drive_train",
        "Cylinders": "inv_inventory|cylinders",
        "BodyStyle": "inv_inventory|body_style",
        "Interiormaterial": "inv_inventory|interior_material",
        "SourceDataDrivetrain": "inv_inventory|source_data_drive_train",
        "SourceDataInteriorMaterialDescription": "inv_inventory|source_data_interior_material_description",
        "SourceDataTransmission": "inv_inventory|source_data_transmission",
        "SourceDataTransmissionSpeed": "inv_inventory|source_data_transmission_speed",
        "TransmissionSpeed": "inv_inventory|transmission_speed",
        "BuildData": "inv_inventory|build_data",
        "HwyMPG": "inv_inventory|highway_mpg",
        "CityMPG": "inv_inventory|city_mpg",
        "VDP": "inv_inventory|vdp",
        "Trim": "inv_inventory|trim",
        "SpecialPrice": "inv_inventory|special_price",
        "Engine": "inv_inventory|engine",
        "EngineDisplacement": "inv_inventory|engine_displacement",
        "FactoryCertified": "inv_inventory|factory_certified",  # C if True, else null
    }
    rows = []
    for entry in unified_inventory:
        try:
            row = {}
            for icc_field, unified_field in field_mappings.items():
                row[icc_field] = entry.get(unified_field)

            # Process equipment and options
            equipment_list = entry.get("inv_equipments|inv_equipments", [])
            standard_equipment = []
            optional_equipment = []
            if equipment_list:
                for equipment in equipment_list:
                    if equipment.get("inv_equipment|is_optional"):
                        optional_equipment.append(equipment.get("inv_equipment|equipment_description"))
                    else:
                        standard_equipment.append(equipment.get("inv_equipment|equipment_description"))

                row["StandardEquipment"] = "|".join(standard_equipment)
                row["OptionalEquipment"] = "|".join(optional_equipment)

            options_list = entry.get("inv_options|inv_options", [])
            option_description = []
            priority_options = []
            if options_list:
                for option in options_list:
                    if option.get("inv_option|is_priority"):
                        priority_options.append(option.get("inv_option|option_description"))
                    else:
                        option_description.append(option.get("inv_option|option_description"))

                row["OptionDescription"] = "|".join(option_description)
                row["PriorityOptions"] = "|".join(priority_options)

            rows.append(row)
        except Exception:
            logger.exception(f"Error processing row: {entry}")
            raise Exception("Error converting unified format to ICC")

    icc_formatted_inventory = pd.DataFrame(rows)

    # Set FactoryCertified to C if True, else null
    icc_formatted_inventory["FactoryCertified"] = icc_formatted_inventory["FactoryCertified"].apply(lambda x: "C" if x else None)

    return icc_formatted_inventory


def get_sftp_secrets(secret_name: Any, secret_key: Any) -> Any:
    """Get SFTP secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data["hostname"], secret_data["port"], secret_data["username"], secret_data["password"]


def record_handler(record: SQSRecord) -> None:
    """Transform and process each record."""
    logger.info(f"Record: {record}")
    try:
        body = loads(record['body'])
        bucket = body["Records"][0]["s3"]["bucket"]["name"]
        key = body["Records"][0]["s3"]["object"]["key"]
        impel_dealer_id = key.split('/')[-1].split('.')[0]
        integration = key.split('/')[1]

        logger.info(f"Impel dealer id: {impel_dealer_id}")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = loads(response['Body'].read())
        # logger.info(f"Content: {content}")

        icc_formatted_inventory = convert_unified_to_icc(content)
        logger.info(f"ICC formatted inventory: {icc_formatted_inventory.head()}")

        # Query RDS database for dealer SFTP info for merch and AI
        rds_instance_obj = RDSInstance()
        ftp_data = rds_instance_obj.select_db_dealer_sftp_details(impel_dealer_id)
        logger.info(f"Dealer FTP details: {ftp_data}")
        if not ftp_data:
            logger.error(f"No FTP data found for dealer: {impel_dealer_id}")
            raise
        merch_dealer_id, salesai_dealer_id, merch_is_active, salesai_is_active = ftp_data[0]

        # Save ICC formatted inventory to S3
        csv_content = icc_formatted_inventory.to_csv(index=False)
        with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file:
            temp_file.write(csv_content)
            temp_file.seek(0)

            # Read CSV content from the temporary file and convert it to bytes
            with open(temp_file.name, 'rb') as file:
                csv_bytes = BytesIO(file.read())

            upload_to_s3(csv_bytes, f"{impel_dealer_id}.csv", integration)

        # Upload to product FTP
        if merch_is_active:
            logger.info(f"Uploading to Merch SFTP: {merch_dealer_id}")
            proccess_and_upload_to_sftp(icc_formatted_inventory, merch_dealer_id, MERCH_SFTP_KEY)

        if salesai_is_active:
            logger.info(f"Uploading to Sales AI SFTP: {salesai_dealer_id}")
            proccess_and_upload_to_sftp(icc_formatted_inventory, salesai_dealer_id, SALESAI_SFTP_KEY)

        if not merch_is_active and not salesai_is_active:
            logger.error(f"No active FTP found for dealer: {impel_dealer_id}")
            raise

    except Exception:
        logger.exception("Error processing record")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Convert to ICC format and syndication to product FTPs."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result

    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
