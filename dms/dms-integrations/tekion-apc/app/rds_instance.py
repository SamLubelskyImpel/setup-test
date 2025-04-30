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

    def get_optin_missing_customers(self, dms_id: str, limit: int):
        """Get list of customers which don't have optin info updated for a dealership."""
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"""
            SELECT c.id, c.dealer_customer_no FROM {self.schema}.consumer AS c
            JOIN {self.schema}.dealer_integration_partner dip ON dip.id = c.dealer_integration_partner_id
            WHERE dip.dms_id = '{dms_id}' AND c.dealer_customer_no IS NOT NULL AND c.dealer_customer_no != ''
            AND c.metadata ->> 'optin_updated' IS NULL
            LIMIT {limit}
        """
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    def update_optin(self, customer_id: str, email: bool, phone: bool, postal_mail: bool, sms: bool):
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"""
            UPDATE {self.schema}.consumer
            SET email_optin_flag = %s,
                phone_optin_flag = %s,
                postal_mail_optin_flag = %s,
                sms_optin_flag = %s
            WHERE id = %s;
        """
        values = (email, phone, postal_mail, sms, customer_id)
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()

    def set_optin_updated_flag(self, customer_id: str):
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"""
            UPDATE {self.schema}.consumer
            SET metadata = jsonb_set(metadata, '{"{optin_updated}"}', 'true', true)
            WHERE id = %s;
        """
        values = (customer_id,)
        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()
