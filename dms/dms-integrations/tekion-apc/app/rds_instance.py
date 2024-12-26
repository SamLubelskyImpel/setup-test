""" Manage RDS connection """
from json import loads
import boto3
import psycopg2
from os import environ

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")


class RDSInstance:
    """Manage RDS connection."""

    def __init__(self):
        self.schema = f"{'prod' if ENVIRONMENT == 'prod' else 'stage'}"
        self.rds_connection = self.get_rds_connection()

    def get_rds_connection(self):
        """Get connection to RDS database."""
        sm_client = boto3.client("secretsmanager")
        secret_string = loads(
            sm_client.get_secret_value(
                SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/RDS/DMS"
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
