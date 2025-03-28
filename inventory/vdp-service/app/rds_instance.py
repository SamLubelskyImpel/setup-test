import logging
from json import loads
from os import environ
import boto3
import psycopg2
import psycopg2.extras

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

    def find_dealer_integration_partner_id(self, provider_dealer_id):
        """Query for DIP ID based on provider dealer ID."""
        query = f"SELECT id FROM {self.schema}.inv_dealer_integration_partner WHERE provider_dealer_id = %s"
        with self.rds_connection.cursor() as cursor:
            cursor.execute(query, (provider_dealer_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def batch_update_inventory_vdp(self, vdp_list, vdp_data_col_list, join_conditions, provider_dealer_id):
        """Batch update inventory VDP data."""
        try:
            update_vdp_query = f"""
                WITH temp_vdp_table AS (
                    SELECT *
                    FROM (VALUES %s) AS data({vdp_data_col_list})
                )
                UPDATE {self.schema}.inv_inventory AS i
                SET vdp = t.vdp_url
                FROM temp_vdp_table AS t
                JOIN {self.schema}.inv_vehicle AS v ON
                    ({join_conditions})
                    AND t.dealer_integration_partner_id = v.dealer_integration_partner_id
                WHERE i.vehicle_id = v.id
                RETURNING i.id;
            """
            with self.rds_connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, update_vdp_query, vdp_list)
                updated_vdp = cursor.fetchall()
                self.rds_connection.commit()
            return updated_vdp if updated_vdp else None
        except Exception as e:
            error_message = f"Error updating VDP records: {provider_dealer_id}.csv - {str(e)}"
            logger.error(error_message)
            raise

    def get_active_dealer_partners(self):
        """Get the active provider dealer IDs."""
        db_active_dealer_partners_query = f"""
            select idip.provider_dealer_id
            from {self.schema}.inv_dealer_integration_partner idip
            join {self.schema}.inv_integration_partner iip on idip.integration_partner_id = iip.id
            where idip.is_active = true
        """
        results = self.execute_rds(db_active_dealer_partners_query)
        db_dealer_ftp_details = results.fetchall()
        if not results:
            return []
        else:
            return db_dealer_ftp_details

    def get_integration_partner_metadata(self, integration_partner_id):
        """Get the integration partner metadata."""
        db_integration_partner_metadata_query = f"""
            select metadata
            from {self.schema}.inv_integration_partner iip
            where impel_integration_partner_id = '{integration_partner_id}'
        """
        results = self.execute_rds(db_integration_partner_metadata_query)
        db_integration_partner_metadata = results.fetchone()
        if not db_integration_partner_metadata:
            return {}
        else:
            return db_integration_partner_metadata[0]
