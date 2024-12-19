import logging
from json import loads
from os import environ

import boto3
import numpy as np
import pandas as pd
import psycopg2

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"
s3_client = boto3.client("s3")


class RDSInstance:
    """Manage RDS connection."""

    def __init__(self, is_prod, integration):
        self.integration = integration
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

    def select_db_dealer_integration_partner_ids(self, dms_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_integration_partner_id_query = f"""
            select dip.id from {self.schema}."dealer_integration_partner" dip
            join {self.schema}."integration_partner" i on dip.integration_partner_id = i.id
            where dip.dms_id = '{dms_id}' and i.impel_integration_partner_id = '{self.integration}' and dip.is_active = true;"""
        results = self.execute_rds(db_dealer_integration_partner_id_query)
        db_dealer_integration_partner_ids = [x[0] for x in results.fetchall()]
        if not results:
            return []
        else:
            return db_dealer_integration_partner_ids

    def select_dealer_id(self, impel_dealer_id):
        """Get the db dealer id for the given unique impel_dealer_id."""
        db_dealer_id_query = f"""
            select d.id from {self.schema}."dealer" d
            where d.impel_dealer_id = '{impel_dealer_id}';"""
        results = self.execute_rds(db_dealer_id_query).fetchone()
        if results is None:
            return None
        else:
            return int(results[0])

    def get_multi_insert_query(self, records, columns, table_name, additional_query=""):
        """Commit several records to the database.
        columns is an array of strings of the column names to insert into
        records is an array of tuples in the same order as columns
        table_name is the 'schema."table_name"'
        additional_query is any query text to append after the insertion
        """
        if len(records) >= 1:
            cursor = self.rds_connection.cursor()
            values_str = f"({', '.join('%s' for _ in range(len(records[0])))})"
            args_str = ",".join(
                cursor.mogrify(values_str, x).decode("utf-8") for x in records
            )
            columns_str = ", ".join(columns)
            query = f"""INSERT INTO {table_name} ({columns_str}) VALUES {args_str} {additional_query}"""
            return query
        else:
            return None

    def get_insert_query_from_df(self, df, table, additional_query=""):
        """Get query from df where df column names are db column names for the given table."""
        selected_columns = []
        db_columns = []
        for table_to_column_name in df.columns:
            if table_to_column_name.split("|")[0] == table:
                selected_columns.append(table_to_column_name)
                db_columns.append(table_to_column_name.split("|")[1])

        column_data = []

        insertion_df = df.copy()
        insertion_df.replace({pd.NaT: None, np.nan: None}, inplace=True)
        for _, row in insertion_df[selected_columns].iterrows():
            data = []
            for x in row.to_numpy():
                # Convert numpy data to python data for psycopg2 mogrify
                if isinstance(x, np.ndarray):
                    x = x.tolist()
                elif isinstance(x, np.integer):
                    x = int(x)
                data.append(x)
            column_data.append(tuple(data))
        table_name = f'{self.schema}."{table}"'
        query = self.get_multi_insert_query(
            column_data, db_columns, table_name, additional_query
        )
        return query

    def insert_table_from_df(
        self, df, table, additional_query="", expect_all_inserted=True
    ):
        """Insert a table from a dataframe and return inserted ids."""
        additional_query += " RETURNING id"
        query = self.get_insert_query_from_df(df, table, additional_query)
        results = self.commit_rds(query)
        if results is None:
            raise RuntimeError(f"No results from query {query}")
        inserted_ids = [x[0] for x in results.fetchall()]
        logger.info(f"Inserted {len(inserted_ids)} rows for {table}")
        if expect_all_inserted and len(inserted_ids) != len(df):
            raise RuntimeError(
                f"Inserted {len(inserted_ids)} {table} entries but expected {len(df)}"
            )
        return inserted_ids
