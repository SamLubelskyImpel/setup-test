import logging
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from json import loads
from os import environ
from typing import Any
import uuid

import boto3
import pandas as pd

from rds_instance import RDSInstance

INVENTORY_BUCKET = environ.get("INVENTORY_BUCKET")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def upload_to_s3(csv_content, integration, provided_dealer_id):
    """Upload files to S3."""
    timestamp = datetime.now(tz=timezone.utc)
    date_key = timestamp.strftime('%Y/%m/%d')
    iso_timestamp = datetime.isoformat('%Y%m%dT%H%M%S')
    unique_id = uuid.uuid4()
    filename = f"{iso_timestamp}_{unique_id}"

    s3_key = f"icc/{integration}/{provided_dealer_id}/{date_key}/{filename}.csv"
    s3_client.put_object(
        Bucket=INVENTORY_BUCKET,
        Key=s3_key,
        Body=csv_content.encode("utf-8")
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
        "ListPrice": "inv_inventory|list_price",
        "SpecialPrice": "inv_inventory|special_price",
        "SourceDataTransmission": "inv_inventory|source_data_transmission",
        "SourceDataTransmissionSpeed": "inv_inventory|source_data_transmission_speed",
        "TransmissionSpeed": "inv_inventory|transmission_speed",
        "BuildData": "inv_inventory|build_data",
        "HwyMPG": "inv_inventory|highway_mpg",
        "CityMPG": "inv_inventory|city_mpg",
        "VDP": "inv_inventory|vdp",
        "Trim": "inv_inventory|trim",
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

            options = entry.get("inv_inventory|options", [])
            row["OptionDescription"] = "|".join(options) if options else None

            priority_options = entry.get("inv_inventory|priority_options", [])
            row["PriorityOptions"] = "|".join(priority_options) if priority_options else None

            equipment_descriptions = entry.get("inv_equipment|equipment_description", [])
            is_optional_flags = entry.get("inv_equipment|is_optional", [])

            # Combine equipment descriptions and is_optional flags
            optional_equipment = []
            standard_equipment = []

            for description, is_optional in zip(equipment_descriptions, is_optional_flags):
                if description is not None:
                    if is_optional:
                        optional_equipment.append(description)
                    else:
                        standard_equipment.append(description)

            row["OptionalEquipment"] = "|".join(optional_equipment) if optional_equipment else None
            row["StandardEquipment"] = "|".join(standard_equipment) if standard_equipment else None

            # Set FactoryCertified to C if True, else null
            row["FactoryCertified"] = "C" if row["FactoryCertified"] else None

            rows.append(row)
        except Exception:
            logger.exception(f"Error processing row: {entry}")
            raise Exception("Error converting unified format to ICC")

    icc_formatted_inventory = pd.DataFrame(rows)
    return icc_formatted_inventory


def lambda_handler(event: Any, context: Any) -> Any:
    """Convert to ICC format and upload to inventory/icc bucket."""
    logger.info(f"Record: {event}")
    try:
        integration_partner = event.get("integration_partner")

        # Initialize RDS instance
        rds_instance = RDSInstance()

        # Retreive active dealer integration partners
        active_dips = rds_instance.get_active_dealer_integration_partners(integration_partner)

        if not active_dips:
            raise ValueError("No active dealer integration partners for integration partner")

        for dip, provider_dealer_id in active_dips:
            inv_data = rds_instance.get_on_lot_inventory(dip)

            if inv_data:
                icc_formatted_inventory = convert_unified_to_icc(inv_data)
                logger.info(f"ICC formatted inventory: {icc_formatted_inventory.head()}")

                # Save ICC formatted inventory to S3
                csv_content = icc_formatted_inventory.to_csv(index=False)
                upload_to_s3(csv_content, integration_partner, provider_dealer_id)
    except Exception:
        logger.exception("Error processing record")
        raise
