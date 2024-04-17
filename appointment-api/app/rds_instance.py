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

    def get_table_names(self):
        """Get a list of table names in the database."""
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        results = self.execute_rds(query)
        table_names = [row[0] for row in results.fetchall()]
        return table_names

    def get_dealer_integration_partner(self, dealer_integration_partner_id):
        query = f"""
        SELECT adip.id, adip.product_id, ad.timezone, adip.integration_dealer_id, aip.metadata FROM {self.schema}.appt_dealer_integration_partner adip
        JOIN {self.schema}.appt_dealer ad ON adip.dealer_id = ad.id
        JOIN {self.schema}.appt_integration_partner aip ON adip.integration_partner_id = aip.id
        WHERE adip.id={dealer_integration_partner_id} AND adip.is_active=true
        """
        results = self.execute_rds(query)
        dealer_partner = results.fetchone()
        return dealer_partner

    def get_vendor_op_code(self, dealer_integration_partner_id, op_code, product_id):
        query = f"""
        SELECT aop.op_code, aoca.id FROM {self.schema}.appt_op_code aoc
        JOIN {self.schema}.appt_op_code_appointment aoca ON aoc.id = aoca.op_code_id
        JOIN {self.schema}.appt_op_code_product ocp ON aoca.op_code_product_id = ocp.id
        WHERE aoc.dealer_integration_partner_id = {dealer_integration_partner_id}
        AND ocp.product_id = {product_id}
        AND ocp.op_code = '{op_code}'
        """
        results = self.execute_rds(query)
        vendor_op_code = [row[0] for row in results.fetchall()]
        if vendor_op_code:
            return vendor_op_code[0], vendor_op_code[1]

        return None, None

    def get_product_op_code(self, dealer_integration_partner_id, vendor_op_code, product_id):
        query = f"""
        SELECT aocp.op_code FROM {self.schema}.appt_op_code_product aocp
        JOIN {self.schema}.appt_op_code_appointment aoca ON aocp.id = aoca.op_code_product_id
        JOIN {self.schema}.appt_op_code aoc ON aoca.op_code_id = aoc.id
        WHERE aocp.product_id = {product_id}
        AND aoc.dealer_integration_partner_id = {dealer_integration_partner_id}
        AND aoc.op_code = '{vendor_op_code}'
        """
        results = self.execute_rds(query)
        dealer_op_codes = results.fetchall()
        if not results:
            return []
        else:
            return dealer_op_codes

    def get_unified_column_names(self):
        """Get a list of column names from all database tables in unified format."""
        unified_column_names = []
        tables = self.get_table_names()
        for table in tables:
            columns = self.get_table_column_names(table)
            for column in columns:
                unified_column_names.append(f"{table}|{column}")
        return unified_column_names

    def get_consumer(self, consumer_id):
        query = f"""
        SELECT id FROM {self.schema}.appt_consumer WHERE id = {consumer_id}
        """
        results = self.execute_rds(query)
        consumer = results.fetchone()
        return consumer[0] if consumer else None

    def create_consumer(self, consumer):
        consumer = {key: value for key, value in consumer.items() if value is not None}
        keys = consumer.keys()
        values = tuple(consumer.values())

        query = f"""
        INSERT INTO {self.schema}.appt_consumer ({', '.join(keys)}) VALUES ({', '.join(values)}) RETURNING id;
        """
        results = self.execute_rds(query)
        consumer = results.fetchone()
        return consumer[0] if consumer else None

    def create_vehicle(self, vehicle):
        vehicle = {key: value for key, value in vehicle.items() if value is not None}
        keys = vehicle.keys()
        values = tuple(vehicle.values())

        query = f"""
        INSERT INTO {self.schema}.appt_vehicle ({', '.join(keys)}) VALUES ({', '.join(values)}) RETURNING id;
        """
        results = self.execute_rds(query)
        vehicle = results.fetchone()
        return vehicle[0] if vehicle else None

    def create_appointment(self, appointment):
        appointment = {key: value for key, value in appointment.items() if value is not None}
        keys = appointment.keys()
        values = tuple(appointment.values())

        query = f"""
        INSERT INTO {self.schema}.appt_appointment ({', '.join(keys)}) VALUES ({', '.join(values)}) RETURNING id;
        """
        results = self.execute_rds(query)
        appointment = results.fetchone()
        return appointment[0] if appointment else None

    def retrieve_appointments(self, dealer_integration_partner_id, first_name, last_name, email_address, phone_number, vin):
        query = f"""
        SELECT * FROM {self.schema}.appt_appointment aa
        JOIN {self.schema}.appt_consumer ac ON aa.consumer_id = ac.id
        JOIN {self.schema}.appt_vehicle av ON aa.vehicle_id = av.id
        WHERE aa.dealer_integration_partner_id = {dealer_integration_partner_id}
        """
        if vin:
            query += f" AND av.vin = '{vin}'"
        if first_name:
            query += f" AND LOWER(ac.first_name) LIKE LOWER('%{first_name}%')"
        if last_name:
            query += f" AND LOWER(ac.last_name) LIKE LOWER('%{last_name}%')"
        if email_address:
            query += f" AND LOWER(ac.email_address) LIKE LOWER('%{email_address}%')"
        if phone_number:
            query += f" AND ac.phone_number = '{phone_number}'"

        results = self.execute_rds(query)
        appointments = results.fetchall()
        if not results:
            return []
        else:
            return appointments
