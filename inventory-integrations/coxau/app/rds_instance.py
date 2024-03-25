import logging
from json import loads
from os import environ

import boto3
import psycopg2

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"
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

    def select_db_active_dealer_partners(self, integration_partner_name):
        """Get the active provider dealer IDs for the given integration partner name."""
        db_active_dealer_partners_query = f"""
            select idip.provider_dealer_id
            from {self.schema}.inv_dealer_integration_partner idip
            join {self.schema}.inv_integration_partner iip on idip.integration_partner_id = iip.id
            where iip.impel_integration_partner_id = '{integration_partner_name}' and idip.is_active = true
        """
        results = self.execute_rds(db_active_dealer_partners_query)
        db_dealer_ftp_details = results.fetchall()
        if not results:
            return []
        else:
            return db_dealer_ftp_details

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
