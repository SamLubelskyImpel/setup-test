import logging
from json import loads
from os import environ
import boto3
import psycopg2
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
                SecretId="prod/DMSDB" if self.is_prod else "test/DMSDB"
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
            where idipv.provider_dealer_id = '{provider_dealer_id}' and idipv.is_active"""
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
            where idipv.provider_dealer_id = '{provider_dealer_id}' and idipv.is_active"""
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

    def insert_and_get_id(self, table, data, returning_col="id"):
        """Insert a record into a table and return the specified column (default 'id')."""
        keys = data.keys()
        values = tuple(data.values())

        if returning_col:
            query = f"INSERT INTO {self.schema}.{table} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(values))}) RETURNING {returning_col};"
        else:
            query = f"INSERT INTO {self.schema}.{table} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(values))});"

        try:
            with self.rds_connection.cursor() as cursor:
                cursor.execute(query, values)
                self.rds_connection.commit()
                if returning_col:
                    result = cursor.fetchone()
                    return result[0] if result else None
        except Exception as e:
            self.rds_connection.rollback()
            raise e

    def is_new_data(self, incoming_received_datetime, provider_dealer_id):
        # Verify that incoming_received_datetime is a string and is not empty
        if not isinstance(incoming_received_datetime, str) or not incoming_received_datetime:
            raise ValueError(f"Incoming received datetime must be a non-empty string, got {type(incoming_received_datetime)}")
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
                if latest_datetime.tzinfo is None or latest_datetime.tzinfo.utcoffset(latest_datetime) is None:
                    latest_datetime = latest_datetime.replace(tzinfo=timezone.utc)

                # Parse incoming datetime string to offset-aware datetime
                incoming_datetime_obj = datetime.strptime(incoming_received_datetime, "%Y-%m-%dT%H:%M:%SZ")
                incoming_datetime_obj = incoming_datetime_obj.replace(tzinfo=timezone.utc)

                is_newer = latest_datetime < incoming_datetime_obj
                return is_newer

        except Exception as e:
            logger.error(f"Error during database query: {e}")
            raise

    def update_dealers_other_vehicles(self, dealer_integration_partner_id, current_feed_inventory_ids):
        """Set the inv_inventory record's on_lot to false for vehicles associated with the dealer not in the current_feed_inventory_ids list, but only if on_lot is currently true."""
        try:
            with self.rds_connection.cursor() as cursor:
                select_query = f"""
                SELECT id FROM {self.schema}.inv_inventory
                WHERE dealer_integration_partner_id = %s AND on_lot = TRUE
                AND id NOT IN %s;
                """
                cursor.execute(select_query, (dealer_integration_partner_id, tuple(current_feed_inventory_ids)))
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
            logger.error(f"Error updating on_lot status for dealer {dealer_integration_partner_id}: {e}")
            self.rds_connection.rollback()

    def check_existing_record(self, table, check_columns, data):
        """Check for an existing record in the database."""
        placeholders = ' AND '.join([f"{col} = %s" for col in check_columns])
        query = f"SELECT id FROM {self.schema}.{table} WHERE {placeholders}"
        values = [data[col] for col in check_columns]

        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, values)
            result = cursor.fetchone()
            return result[0] if result else None

    def insert_unique_record(self, table, data, unique_columns):
        """Insert a record if it does not exist, and return its ID."""
        existing_id = self.check_existing_record(table, unique_columns, data)
        if existing_id:
            return existing_id
        else:
            return self.insert_and_get_id(table, data)

    def find_dealer_integration_partner_id(self, provider_dealer_id):
        query = f"SELECT id FROM {self.schema}.inv_dealer_integration_partner WHERE provider_dealer_id = %s"
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, (provider_dealer_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def insert_vehicle(self, vehicle_data):
        """Insert a vehicle record if it doesn't exist based on unique attributes, or update the existing record if it does."""
        unique_columns = ['vin', 'model', 'stock_num', 'dealer_integration_partner_id', 'mileage']

        # Prepare data for checking existing record
        check_data = {col: vehicle_data.get(col) for col in unique_columns if col in vehicle_data}

        existing_vehicle_id = self.check_existing_record("inv_vehicle", list(check_data.keys()), check_data)
        if existing_vehicle_id:
            # Columns to update, excluding unique columns and 'id'
            update_columns = {'type', 'new_or_used', 'oem_name', 'make', 'year'}
            set_clause = ', '.join([f"{col} = %s" for col in update_columns if col in vehicle_data])
            update_query = f"""
            UPDATE {self.schema}.inv_vehicle
            SET {set_clause}
            WHERE id = %s;
            """
            update_values = [vehicle_data[col] for col in update_columns if col in vehicle_data]
            update_values.append(existing_vehicle_id)

            with self.rds_connection.cursor() as cursor:
                cursor.execute(update_query, update_values)
                self.rds_connection.commit()
                # logger.info(f"Updated existing vehicle record ID: {existing_vehicle_id} with new data.")
            return existing_vehicle_id
        else:
            return self.insert_and_get_id("inv_vehicle", vehicle_data)

    def insert_inventory_item(self, inventory_data):
        """Insert an inventory item record if it doesn't exist based on unique attributes, or update the existing record if it does."""
        unique_columns = ['vehicle_id', 'dealer_integration_partner_id']

        check_data = {col: inventory_data.get(col) for col in unique_columns if col in inventory_data}
        existing_inventory_id = self.check_existing_record("inv_inventory", list(check_data.keys()), check_data)
        if existing_inventory_id:
            update_columns = {
                'list_price', 'special_price', 'fuel_type', 'exterior_color', 'interior_color', 'doors',
                'seats', 'transmission', 'drive_train', 'cylinders', 'body_style',
                'series', 'vin', 'interior_material', 'trim', 'factory_certified',
                'region', 'on_lot', 'metadata', 'received_datetime', 'photo_url', 'vdp', 'comments'
            }
            set_clause = ', '.join([f"{col} = %s" for col in update_columns if col in inventory_data])
            update_query = f"""
            UPDATE {self.schema}.inv_inventory
            SET {set_clause}
            WHERE id = %s;
            """
            update_values = [inventory_data[col] for col in update_columns if col in inventory_data]
            update_values.append(existing_inventory_id)

            with self.rds_connection.cursor() as cursor:
                cursor.execute(update_query, update_values)
                self.rds_connection.commit()
                # logger.info(f"Updated existing inventory record ID: {existing_inventory_id} with new data.")
            return existing_inventory_id
        else:
            return self.insert_and_get_id("inv_inventory", inventory_data)

    def insert_option(self, option_data):
        """Insert an option record if it does not exist, ensuring no duplicates based on description and priority."""
        try:
            existing_option_id = self.check_existing_record(
                table="inv_option",
                check_columns=["option_description", "is_priority"],
                data=option_data
            )

            if existing_option_id:
                return existing_option_id
            else:
                return self.insert_and_get_id("inv_option", option_data)
        except Exception as e:
            self.rds_connection.rollback()
            raise e

    def link_option_to_inventory(self, inventory_id, option_ids):
        """Link options to an inventory item via inv_option_inventory, avoiding duplicates."""
        for option_id in option_ids:
            # Check if the link already exists
            existing_link = self.check_existing_record(
                table="inv_option_inventory",
                check_columns=["inv_inventory_id", "inv_option_id"],
                data={"inv_inventory_id": inventory_id, "inv_option_id": option_id}
            )

            if not existing_link:
                # Only insert if the link does not exist
                self.insert_and_get_id("inv_option_inventory", {"inv_inventory_id": inventory_id, "inv_option_id": option_id}, returning_col=None)
            else:
                query = f"SELECT option_description, is_priority FROM {self.schema}.inv_option WHERE id = %s"
                with self.rds_connection.cursor() as cursor:
                    cursor.execute(query, (option_id,))
                    result = cursor.fetchone()
                    # uncomment for debugging
                    # option_description, is_priority = result if result else ("N/A", "N/A")
                    # logger.warning(f"Link between inventory ID {inventory_id} and option ID {option_id} already exists, skipping. Option description: '{option_description}', is_priority: {is_priority}")
