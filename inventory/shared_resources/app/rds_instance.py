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


class RDSInstance:
    """Manage RDS connection."""

    def __init__(self):
        self.is_prod = IS_PROD
        self.schema = f"{'prod' if self.is_prod else 'stage'}"
        self.rds_connection = self.get_rds_connection()

    def get_rds_connection(self):
        """Get connection to RDS database."""
        sm_client = boto3.client("secretsmanager")
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

    def select_db_dealer_sftp_details(self, provider_dealer_id):
        """Get the db dealer id for the given provider dealer id."""
        db_dealer_sftp_details_query = f"""
            select iv.merch_dealer_id, iv.salesai_dealer_id, iv.merch_is_active, iv.salesai_is_active
            from {self.schema}.inv_dealer iv
            join {self.schema}.inv_dealer_integration_partner idipv on idipv.dealer_id = iv.id
            where idipv.provider_dealer_id = '{provider_dealer_id}' and idipv.is_active;
        """
        results = self.execute_rds(db_dealer_sftp_details_query)
        db_dealer_sftp_details = results.fetchall()
        if not results:
            return []
        else:
            return db_dealer_sftp_details

    def select_db_dip_metadata(self, provider_dealer_id) -> dict:
        """Get the db dip metadata for the given provider dealer id."""
        query = f"""
            select idipv.metadata
            from {self.schema}.inv_dealer_integration_partner idipv
            where idipv.provider_dealer_id = '{provider_dealer_id}' and idipv.is_active;
        """
        results = self.execute_rds(query)
        metadata = results.fetchone()
        if not metadata:
            return {}
        else:
            return metadata[0]

    def get_table_names(self):
        """Get a list of table names in the database."""
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        results = self.execute_rds(query)
        table_names = [row[0] for row in results.fetchall()]
        return table_names

    def get_table_column_names(self, table_name):
        """Get a list of column names in the given database table."""
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        results = self.execute_rds(query)
        column_names = [row[0] for row in results.fetchall()]
        return column_names

    def get_unified_column_names(self):
        """Get a list of column names from all database tables in unified format."""
        unified_column_names = []
        tables = self.get_table_names()
        for table in tables:
            columns = self.get_table_column_names(table)
            for column in columns:
                unified_column_names.append(f"{table}|{column}")
        return unified_column_names

    def is_new_data(self, incoming_received_datetime, provider_dealer_id):
        """Compare incoming data's received datetime with the latest datetime in the database."""
        # Verify that incoming_received_datetime is a string and is not empty
        if (
            not isinstance(incoming_received_datetime, str)
            or not incoming_received_datetime
        ):
            raise ValueError(
                f"Incoming received datetime must be a non-empty string, got {type(incoming_received_datetime)}"
            )
        # Use a simple SQL query for debugging
        query = f"""
        SELECT received_datetime
        FROM {self.schema}.inv_inventory ii
        JOIN {self.schema}.inv_dealer_integration_partner idip ON idip.id = ii.dealer_integration_partner_id
        WHERE idip.provider_dealer_id = '{provider_dealer_id}' and ii.on_lot = TRUE
        ORDER BY received_datetime DESC LIMIT 1;
        """
        try:
            with self.rds_connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                # If there's no result, then the database is empty, and the data is new
                if not result:
                    return True
                latest_datetime = result[0]

                # Make sure the latest_datetime is offset-aware
                if (
                    latest_datetime.tzinfo is None
                    or latest_datetime.tzinfo.utcoffset(latest_datetime) is None
                ):
                    latest_datetime = latest_datetime.replace(tzinfo=timezone.utc)

                # Parse incoming datetime string to offset-aware datetime
                incoming_datetime_obj = datetime.strptime(
                    incoming_received_datetime, "%Y-%m-%dT%H:%M:%SZ"
                )
                incoming_datetime_obj = incoming_datetime_obj.replace(
                    tzinfo=timezone.utc
                )

                is_newer = latest_datetime < incoming_datetime_obj
                return is_newer

        except Exception as e:
            logger.error(f"Error during database query: {e}")
            raise

    def update_dealers_other_vehicles(
        self, dealer_integration_partner_id, current_feed_inventory_ids
    ):
        """
        Set the inv_inventory record's on_lot to false for vehicles associated with the dealer not
        in the current_feed_inventory_ids list, but only if on_lot is currently true.
        """
        try:
            with self.rds_connection.cursor() as cursor:
                select_query = f"""
                SELECT id FROM {self.schema}.inv_inventory
                WHERE dealer_integration_partner_id = %s AND on_lot = TRUE
                AND id NOT IN %s;
                """
                cursor.execute(
                    select_query,
                    (dealer_integration_partner_id, tuple(current_feed_inventory_ids)),
                )
                records_to_update = cursor.fetchall()
                records_to_update_ids = [record[0] for record in records_to_update]

                # If there are no records to update, exit early
                if not records_to_update_ids:
                    return

                # Step 2: Update those records by setting on_lot to FALSE
                update_query = f"""
                UPDATE {self.schema}.inv_inventory
                SET on_lot = FALSE
                WHERE id IN %s;
                """
                cursor.execute(update_query, (tuple(records_to_update_ids),))
                self.rds_connection.commit()

        except Exception as e:
            logger.error(
                f"Error updating on_lot status for dealer {dealer_integration_partner_id}: {e}"
            )
            self.rds_connection.rollback()

    def find_dealer_integration_partner_id(self, provider_dealer_id):
        """Query for DIP ID based on provider dealer ID."""
        query = f"SELECT id FROM {self.schema}.inv_dealer_integration_partner WHERE provider_dealer_id = %s"
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, (provider_dealer_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def batch_upsert_vehicles(self, vehicle_data_list, dealer_integration_partner_id):
        """Batch upsert vehicles by pre-checking for existing records."""
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

        # Step 2: Separate records for update and insert
        update_data = []
        insert_data = []

        for v in vehicle_data_list:
            key = (v['vin'], dealer_integration_partner_id, v['model'], v['stock_num'], v['mileage'])
            if key in existing_vehicle_map:
                # Update record if it exists
                v['id'] = existing_vehicle_map[key]  # Add ID for updating
                update_data.append(v)
            else:
                # Otherwise, insert as a new record
                insert_data.append(v)

        if len(update_data) + len(insert_data) != len(vehicle_data_list):
            logger.info(f"update_data: {update_data}")
            logger.info(f"insert_data: {insert_data}")
            raise ValueError("Mismatch between update and insert data lengths.")

        # Step 3: Perform batch updates and inserts
        if update_data:
            update_query = f"""
            UPDATE {self.schema}.inv_vehicle AS v SET
                type = data.type,
                new_or_used = data.new_or_used,
                oem_name = data.oem_name,
                make = data.make,
                year = data.year
            FROM (VALUES %s) AS data(id, type, new_or_used, oem_name, make, year)
            WHERE v.id = data.id;
            """
            update_values = [
                (
                    v['id'], v.get('type'), v.get('new_or_used'),
                    v.get('oem_name'), v.get('make'), v.get('year')
                )
                for v in update_data
            ]
            with self.rds_connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, update_query, update_values)
                self.rds_connection.commit()

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

        if len(new_vehicle_records) != len(insert_data):
            logger.info(f"New vehicles: {len(new_vehicle_records)}")
            raise ValueError("Mismatch between inserted records and new vehicle records.")

        # Add newly inserted records to the map
        for v in new_vehicle_records:
            key = (v[1], v[2], v[3], v[4], v[5])
            existing_vehicle_map[key] = v[0]

        # Return the map of unique keys to vehicle IDs
        return existing_vehicle_map

    def batch_insert_inventory(self, inventory_data_list, dealer_integration_partner_id):
        """Batch insert or update inventory items based on vehicle_id and dealer_integration_partner_id."""
        inventory_ids = set()

        insert_query = f"""
        INSERT INTO {self.schema}.inv_inventory (vehicle_id, dealer_integration_partner_id, list_price, special_price, fuel_type, exterior_color, 
        interior_color, doors, seats, transmission, drive_train, cylinders, body_style, series, vin, interior_material, trim, 
        factory_certified, region, on_lot, metadata, received_datetime, photo_url, vdp, comments, options, priority_options)
        VALUES %s
        ON CONFLICT (vehicle_id, dealer_integration_partner_id) DO UPDATE SET
            list_price = EXCLUDED.list_price,
            special_price = EXCLUDED.special_price,
            fuel_type = EXCLUDED.fuel_type,
            exterior_color = EXCLUDED.exterior_color,
            interior_color = EXCLUDED.interior_color,
            doors = EXCLUDED.doors,
            seats = EXCLUDED.seats,
            transmission = EXCLUDED.transmission,
            drive_train = EXCLUDED.drive_train,
            cylinders = EXCLUDED.cylinders,
            body_style = EXCLUDED.body_style,
            series = EXCLUDED.series,
            vin = EXCLUDED.vin,
            interior_material = EXCLUDED.interior_material,
            trim = EXCLUDED.trim,
            factory_certified = EXCLUDED.factory_certified,
            region = EXCLUDED.region,
            on_lot = EXCLUDED.on_lot,
            metadata = EXCLUDED.metadata,
            received_datetime = EXCLUDED.received_datetime,
            photo_url = EXCLUDED.photo_url,
            vdp = EXCLUDED.vdp,
            comments = EXCLUDED.comments,
            options = EXCLUDED.options,
            priority_options = EXCLUDED.priority_options
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

        if len(new_inventory_records) != len(inventory_data_list):
            logger.info(f"New inventory IDs: {len(new_inventory_records)}")
            logger.error(f"Number of processed inventory IDs does not match the number of records available. Expected {len(inventory_data_list)}")
            raise ValueError("Mismatch between existing and new inventory records.")

        return inventory_ids
