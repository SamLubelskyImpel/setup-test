import logging
from json import loads
from os import environ

import boto3
# import numpy as np
# import pandas as pd
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

    def select_db_dealer_ftp_details(self, impel_dealer_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_ftp_details_query = f"""
            select iv.merch_dealer_id, iv.ai_dealer_id, iv.is_active_merch, iv.is_active_ai
            from {self.schema}.inv_dealer iv
            where iv.impel_dealer_id = '{impel_dealer_id}'"""
        results = self.execute_rds(db_dealer_ftp_details_query)
        db_dealer_ftp_details = results.fetchall()
        if not results:
            return []
        else:
            return db_dealer_ftp_details
