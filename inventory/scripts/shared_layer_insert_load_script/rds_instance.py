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

    def check_existing_record(self, table, check_columns, data):
        """Check for an existing record in the database."""
        placeholders = " AND ".join([f"{col} = %s" for col in check_columns])
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
        """
        Insert a vehicle record if it doesn't exist based on unique attributes,
        do not update the existing record if it does.
        """
        unique_columns = [
            "vin",
            "model",
            "stock_num",
            "dealer_integration_partner_id",
        ]

        # Prepare data for checking existing record
        check_data = {
            col: vehicle_data.get(col) for col in unique_columns if col in vehicle_data
        }

        existing_vehicle_id = self.check_existing_record(
            "inv_vehicle", list(check_data.keys()), check_data
        )
        if not existing_vehicle_id:
            return self.insert_and_get_id("inv_vehicle", vehicle_data)

    def insert_inventory_item(self, inventory_data):
        """
        Insert an inventory item record if the vehicle_id does not exist in the database,
        """
        unique_columns = ["vehicle_id", "dealer_integration_partner_id"]

        check_data = {
            col: inventory_data.get(col)
            for col in unique_columns
            if col in inventory_data
        }
        existing_inventory_id = self.check_existing_record(
            "inv_inventory", list(check_data.keys()), check_data
        )
        if existing_inventory_id:
            return existing_inventory_id
        else:
            return self.insert_and_get_id("inv_inventory", inventory_data)

    def link_option_to_inventory(self, inventory_id, option_ids):
        """Link options to an inventory item via inv_option_inventory, avoiding duplicates."""
        try:
            with self.rds_connection.cursor() as cursor:
                insert_query = f"""
                INSERT INTO {self.schema}.inv_option_inventory (inv_inventory_id, inv_option_id)
                VALUES %s
                ON CONFLICT DO NOTHING
                """
                values = [(inventory_id, option_id) for option_id in option_ids]
                psycopg2.extras.execute_values(
                    cursor, insert_query, values, template=None, page_size=100
                )
                self.rds_connection.commit()
        except Exception as e:
            self.rds_connection.rollback()
            raise e

    def bulk_check_existing_records(self, table, check_columns, data_list):
        """Check for existing records using a bulk transaction."""
        try:
            # Construct the WHERE clause for the bulk check
            values = []
            for data in data_list:
                values.append(tuple([data[col] for col in check_columns]))

            where_clause = f"({', '.join(check_columns)}) IN %s"

            query = f"""
                SELECT id, {', '.join(check_columns)}
                FROM {self.schema}.{table}
                WHERE {where_clause}
            """

            with self.rds_connection.cursor() as cursor:
                cursor.execute(query, (tuple(values),))
                results = cursor.fetchall()

                # Map the results to a dictionary
                existing_records = {
                    tuple(result[1:]): result[0] for result in results
                }

            return existing_records
        except Exception as e:
            self.rds_connection.rollback()
            raise e

    def insert_options(self, option_data_list):
        """Insert multiple option records in a single bulk transaction."""
        try:
            # Check for existing options and filter out duplicates
            existing_records = self.bulk_check_existing_records(
                table="inv_option",
                check_columns=["option_description", "is_priority"],
                data_list=option_data_list,
            )

            existing_option_ids = []
            new_option_data_list = []
            for option_data in option_data_list:
                key = (option_data["option_description"], option_data["is_priority"])
                if key in existing_records:
                    existing_option_ids.append(existing_records[key])
                else:
                    new_option_data_list.append(option_data)

            # Perform bulk insert for new options
            if new_option_data_list:
                new_option_data_list_values = [
                    (option_data["option_description"], option_data["is_priority"])
                    for option_data in new_option_data_list
                ]

                with self.rds_connection.cursor() as cursor:
                    insert_query = f"""
                    INSERT INTO {self.schema}.inv_option (option_description, is_priority)
                    VALUES %s
                    ON CONFLICT (option_description, is_priority) DO NOTHING
                    RETURNING id
                    """
                    psycopg2.extras.execute_values(
                        cursor,
                        insert_query,
                        new_option_data_list_values,
                        template="(%s, %s)",
                        page_size=100,
                    )
                    new_option_ids = [row[0] for row in cursor.fetchall()]
                    self.rds_connection.commit()
            else:
                new_option_ids = []

            return existing_option_ids + new_option_ids
        except Exception as e:
            self.rds_connection.rollback()
            raise e
