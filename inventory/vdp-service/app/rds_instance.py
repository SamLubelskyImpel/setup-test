import logging
from json import loads
from os import environ

import boto3
import psycopg2

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
