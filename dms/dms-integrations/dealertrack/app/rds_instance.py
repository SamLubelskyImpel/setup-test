""" Manage RDS connection """
from json import loads, dumps
import boto3
import psycopg2


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
                SecretId="prod/RDS/DMS" if self.is_prod else "test/RDS/DMS"
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


    def select_db_active_dealer_partners(self, integration_partner_name):
        """Get the active provider dealer IDs for the given integration partner name."""
        results = []

        query = f"""
            SELECT d.id, dip.dms_id FROM {self.schema}.dealer AS d
            JOIN {self.schema}.dealer_integration_partner AS dip ON d.id = dip.dealer_id
            JOIN {self.schema}.integration_partner AS ip ON dip.integration_partner_id = ip.id
            WHERE dip.is_active = TRUE AND ip.impel_integration_partner_id = '{integration_partner_name}'
        """

        with self.rds_connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return results


    def select_db_historical_active_dealer_partners(self, integration_partner_name):
        """Get the active provider dealer IDs with historical pull enabled for the given integration partner name."""
        results = []

        query = f"""
            SELECT dip.dms_id FROM {self.schema}.dealer AS d
            JOIN {self.schema}.dealer_integration_partner AS dip ON d.id = dip.dealer_id
            JOIN {self.schema}.integration_partner AS ip ON dip.integration_partner_id = ip.id
            WHERE dip.is_active = TRUE
            AND ip.impel_integration_partner_id = '{integration_partner_name}'
            AND dip.metadata -> 'historical_pull' ->> 'status' in ('ENABLED')
        """

        with self.rds_connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return [dms_id for dms_id, in results]

    def save_historical_progress(self, dms_ids: list[str], historical_progress: dict):
        """Update the metadata historical_pull record to keep track of its progress."""
        ids = [f"'{id}'" for id in dms_ids]
        ids = f"{','.join(ids)}"
        query = f"""
            UPDATE {self.schema}.dealer_integration_partner
            SET metadata = jsonb_set(metadata::jsonb, '{"{historical_pull}"}', '{dumps(historical_progress)}'::jsonb, true)
            WHERE dms_id IN ({ids})
        """

        with self.rds_connection.cursor() as cursor:
            cursor.execute(query)

        self.rds_connection.commit()
