""" Manage RDS connection """
from json import loads

import boto3
import psycopg2
from psycopg2.extras import execute_values


class RDSInstance:
    """Manage RDS connection."""

    def __init__(self, is_prod):
        self.is_prod = is_prod
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

    def get_table_names(self):
        """Get a list of table names in the database."""
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        cursor.execute(query)
        table_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return table_names

    def get_table_column_names(self, table_name):
        """Get a list of column names in the given database table."""
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        cursor.execute(query)
        column_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
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
    
    def execute_query(self, query, data=None):
        """Execute a SQL query."""
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, data)
            self.rds_connection.commit()
    
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

    def batch_insert_from_df(self, table_name, df):
        """Insert DataFrame data into the specified table using batch operation."""
        with self.rds_connection.cursor() as cursor:
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = ','.join(list(df.columns))
            query = f"INSERT INTO {table_name}({cols}) VALUES %s ON CONFLICT DO NOTHING"
            execute_values(cursor, query, tuples)
            self.rds_connection.commit()

    def check_existing_record(self, table, check_columns, data):
        """Check for an existing record in the database."""
        placeholders = ' AND '.join([f"{col} = %s" for col in check_columns])
        query = f"SELECT id FROM {self.schema}.{table} WHERE {placeholders}"
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, tuple(data[col] for col in check_columns))
            result = cursor.fetchone()
            return result[0] if result else None

    def insert_unique_record(self, table, data, unique_columns):
        """Insert a record if it does not exist, and return its ID."""
        existing_id = self.check_existing_record(table, unique_columns, data)
        if existing_id:
            return existing_id
        else:
            return self.insert_and_get_id(table, data)

    def batch_upsert_from_df(self, table_name, df, unique_columns):
        """Insert DataFrame data into the specified table using batch operation and avoiding duplicates."""
        ids = []
        for _, row in df.iterrows():
            data = row.to_dict()
            record_id = self.insert_unique_record(table_name, data, unique_columns)
            ids.append(record_id)
        return ids

    def find_dealer_integration_partner_id(self, provider_dealer_id):
        query = f"SELECT id FROM {self.schema}.inv_dealer_integration_partner WHERE provider_dealer_id = %s"
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, (provider_dealer_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def insert_vehicle(self, vehicle_data):
        """Insert a vehicle record and return its ID."""
        return self.insert_and_get_id("inv_vehicle", vehicle_data)

    def insert_inventory_item(self, inventory_data):
        """Insert an inventory item record and return its ID."""
        return self.insert_and_get_id("inv_inventory", inventory_data)

    def insert_equipment(self, equipment_data):
        """Insert an equipment record and return its ID, ensuring no duplicates."""
        # Define the criteria for a unique equipment
        try:
            unique_criteria = {
                'equipment_description': equipment_data['equipment_description'],
                'is_optional': equipment_data['is_optional']
            }
            
            # Check if an equipment with the same description (and other unique fields if applicable) exists
            existing_id = self.check_existing_record('inv_equipment', list(unique_criteria.keys()), unique_criteria)
            
            if existing_id is not None:
                return existing_id
            else:
                return self.insert_and_get_id("inv_equipment", equipment_data)
        except Exception as e:
            self.rds_connection.rollback()
            raise e

    def insert_option(self, option_data):
        """Insert an option record and return its ID, ensuring no duplicates."""
        try:
            unique_criteria = {
                'option_description': option_data['option_description'],
                'is_priority': option_data['is_priority']
            }
            
            # Check if an option with the same description and priority status exists
            existing_id = self.check_existing_record('inv_option', list(unique_criteria.keys()), unique_criteria)
            
            if existing_id is not None:
                return existing_id
            else:
                return self.insert_and_get_id("inv_option", option_data)
        except Exception as e:
            self.rds_connection.rollback()
            raise e

    def link_equipment_to_inventory(self, inventory_id, equipment_ids):
        """Link equipment to an inventory item via inv_equipment_inventory."""
        for equipment_id in equipment_ids:
            self.insert_and_get_id("inv_equipment_inventory", {"inv_inventory_id": inventory_id, "inv_equipment_id": equipment_id}, returning_col=None)

    def link_option_to_inventory(self, inventory_id, option_ids):
        """Link options to an inventory item via inv_option_inventory."""
        for option_id in option_ids:
            self.insert_and_get_id("inv_option_inventory", {"inv_inventory_id": inventory_id, "inv_option_id": option_id}, returning_col=None)
