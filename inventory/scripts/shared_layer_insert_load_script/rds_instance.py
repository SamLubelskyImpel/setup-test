import logging
from json import loads
from os import environ
import boto3
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
AWS_REGION = environ.get("AWS_REGION", "us-east-1")


class RDSInstance:
    """Manage RDS connection."""
    def __init__(self):
        self.is_prod = IS_PROD
        self.schema = f"{'prod' if self.is_prod else 'stage'}"
        self.rds_connection = self.get_rds_connection()

    def get_rds_connection(self):
        """Get connection to RDS database."""
        sm_client = boto3.client("secretsmanager", region_name=AWS_REGION)
        secret_string = loads(
            sm_client.get_secret_value(
                SecretId="prod/RDS/SHARED" if self.is_prod else "test/RDS/SHARED"
            )["SecretString"]
        )
        return psycopg2.connect(
            user=secret_string["user"],
            password=secret_string["password"],
            host=secret_string["host"],
            port=secret_string["port"],
            database=secret_string["db_name"],
        )

    def execute_rds(self, query_str):
        """Execute query on RDS and return cursor."""
        cursor = self.rds_connection.cursor()
        cursor.execute(query_str)
        return cursor

    def commit_rds(self, query_str):
        """Execute and commit query on RDS and return cursor."""
        cursor = self.execute_rds(query_str)
        self.rds_connection.commit()
        return cursor

    def find_dealer_integration_partner_id(self, provider_dealer_id):
        """Query for DIP ID based on provider dealer ID."""
        query = f"SELECT id FROM {self.schema}.inv_dealer_integration_partner WHERE provider_dealer_id = %s"
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, (provider_dealer_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def batch_upsert_vehicles(self, vehicle_data_list, dealer_integration_partner_id):
        """Batch upsert vehicles only new records."""
        logger.info(f"Batch upserting {len(vehicle_data_list)} vehicles.")

        # Step 1: Pre-batch lookup of existing vehicles by key attributes
        lookup_keys = [(v['vin'], dealer_integration_partner_id, v['model'], v['stock_num'], v['mileage']) for v in vehicle_data_list]
        lookup_query = f"""
        SELECT id, vin, dealer_integration_partner_id, model, stock_num, mileage
        FROM {self.schema}.inv_vehicle
        WHERE (vin, dealer_integration_partner_id, model, stock_num, mileage) IN %s;
        """
        with self.rds_connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, lookup_query, [lookup_keys])
            existing_vehicles = cursor.fetchall()

        # Map existing vehicles by their key attributes to their IDs
        existing_vehicle_map = {
            (v[1], v[2], v[3], v[4], v[5]): v[0]
            for v in existing_vehicles
        }
        logger.info(f"Existing vehicles: {len(existing_vehicle_map)}")

        # Step 2: Separate records for insert
        insert_data = []

        for v in vehicle_data_list:
            key = (v['vin'], dealer_integration_partner_id, v['model'], v['stock_num'], v['mileage'])
            if key not in existing_vehicle_map:
                # Otherwise, insert as a new record
                insert_data.append(v)

        # Step 3: Perform batch inserts
        new_vehicle_records = []
        if insert_data:
            logger.info(f"Inserting {len(insert_data)} new vehicles.")

            insert_query = f"""
            INSERT INTO {self.schema}.inv_vehicle (vin, dealer_integration_partner_id, model, stock_num, mileage, type, new_or_used, oem_name, make, year)
            VALUES %s
            RETURNING id, vin, dealer_integration_partner_id, model, stock_num, mileage;
            """
            # Process in batches to handle large inserts
            for i in range(0, len(insert_data), 100):
                batch = insert_data[i:i+100]
                insert_values = [
                    (v['vin'], dealer_integration_partner_id, v['model'], v['stock_num'], v['mileage'],
                     v['type'], v['new_or_used'], v['oem_name'], v['make'], v['year'])
                    for v in batch
                ]

                with self.rds_connection.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, insert_query, insert_values)
                    batch_results = cursor.fetchall()
                    new_vehicle_records.extend(batch_results)  # Append results of each batch
        else:
            logger.info("No new vehicles to insert.")

        if len(new_vehicle_records) != len(insert_data):
            logger.info(f"New vehicles: {len(new_vehicle_records)}")
            raise ValueError("Mismatch between inserted records and new vehicle records.")

        # Add newly inserted records to the map
        for v in new_vehicle_records:
            key = (v[1], v[2], v[3], v[4], v[5])
            existing_vehicle_map[key] = v[0]

        new_vehicle_ids = [v[0] for v in new_vehicle_records]

        # Return the map of unique keys to vehicle IDs
        return existing_vehicle_map, new_vehicle_ids

    def batch_insert_inventory(self, inventory_data_list, dealer_integration_partner_id):
        """Batch insert or update inventory items based on vehicle_id and dealer_integration_partner_id."""
        inventory_ids = set()

        insert_query = f"""
        INSERT INTO {self.schema}.inv_inventory (vehicle_id, dealer_integration_partner_id, list_price, special_price, fuel_type, exterior_color,
        interior_color, doors, seats, transmission, drive_train, cylinders, body_style, series, vin, interior_material, trim,
        factory_certified, region, on_lot, metadata, received_datetime, photo_url, vdp, comments, options, priority_options)
        VALUES %s
        ON CONFLICT (vehicle_id, dealer_integration_partner_id) DO NOTHING
        RETURNING id;
        """
        new_inventory_records = []

        # Process in batches to handle large inserts
        for i in range(0, len(inventory_data_list), 100):
            batch = inventory_data_list[i:i+100]
            insert_values = [
                (
                    inv['vehicle_id'], dealer_integration_partner_id, inv.get('list_price'), inv.get('special_price'),
                    inv.get('fuel_type'), inv.get('exterior_color'), inv.get('interior_color'), inv.get('doors'), inv.get('seats'),
                    inv.get('transmission'), inv.get('drive_train'), inv.get('cylinders'), inv.get('body_style'), inv.get('series'),
                    inv['vin'], inv.get('interior_material'), inv.get('trim'), inv.get('factory_certified'), inv.get('region'),
                    inv.get('on_lot'), inv.get('metadata'), inv.get('received_datetime'), inv.get('photo_url'), inv.get('vdp'),
                    inv.get('comments'), inv.get('options'), inv.get('priority_options')
                )
                for inv in batch
            ]

            with self.rds_connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, insert_query, insert_values)
                self.rds_connection.commit()
                batch_results = cursor.fetchall()
                new_inventory_records.extend(batch_results)  # Append results of each batch

        logger.info(f"Processed inventory: {len(new_inventory_records)}")
        inventory_ids.update([i[0] for i in new_inventory_records] if new_inventory_records else [])

        return inventory_ids
