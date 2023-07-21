"""Rey Rey ETL Job."""
import logging
import sys
import uuid
from datetime import datetime
from json import dumps, loads

import boto3
import psycopg2
import pyspark
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.gluetypes import ArrayType, ChoiceType, Field, NullType, StructType
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import BooleanType, DoubleType, StringType
from pyspark.sql.window import Window


# TODO: Migrate SQL code to individual lambda
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
        column_names = df.columns
        column_data = []
        for row in df.collect():
            column_data.append(tuple(row))
        table_name = f'{self.schema}."{table}"'
        query = self.get_multi_insert_query(
            column_data, column_names, table_name, additional_query
        )
        return query

    def select_db_dealer_integration_partner_id(self, dms_id):
        """Get the db dealer id for the given dms id."""
        db_dealer_integration_partner_id_query = f"""
            select dip.id from {self.schema}."dealer_integration_partner" dip
            join {self.schema}."integration_partner" i on dip.integration_partner_id = i.id 
            where dip.dms_id = '{dms_id}' and i.impel_integration_partner_id = '{self.integration}' and dip.is_active = true;"""
        results = self.execute_rds(db_dealer_integration_partner_id_query).fetchone()
        if results is None:
            raise RuntimeError(
                f"No active dealer {dms_id} found with query {db_dealer_integration_partner_id_query}."
            )
        else:
            return results[0]

    def insert_op_code_repair_order(self, dealer_df):
        """Given a dataframe of dealer data insert into op code repair order table and return row count."""
        desired_op_code_repair_order_columns = ["op_codes", "repair_order_no"]
        actual_op_code_repair_order_columns = [
            x for x in desired_op_code_repair_order_columns if x in dealer_df.columns
        ]
        actual_op_code_repair_order_columns.append("dealer_integration_partner_id")

        op_code_repair_order_df = (
            dealer_df.select(actual_op_code_repair_order_columns)
            .withColumn("op_codes", F.explode("op_codes"))
            .select(
                F.col("op_codes.op_code").alias("op_code"),
                "dealer_integration_partner_id",
                "repair_order_no",
            )
        )

        conditions_list = []
        for row in op_code_repair_order_df.collect():
            condition = f"""
                sro.dealer_integration_partner_id = {row['dealer_integration_partner_id']} 
                and sro.repair_order_no = '{row['repair_order_no']}' 
                and oc.op_code = '{row['op_code']}'
            """
            conditions_list.append(condition)
        conditions_str = ") or (".join(conditions_list)
        insert_op_code_repair_order_query = f"""
            insert into {self.schema}.op_code_repair_order (op_code_id, repair_order_id)
            select oc.id as op_code_id, sro.id as repair_order_id
            from {self.schema}.service_repair_order sro
            join {self.schema}.op_code oc on oc.dealer_integration_partner_id=sro.dealer_integration_partner_id
            where ({conditions_str})
            RETURNING id
        """
        results = self.commit_rds(insert_op_code_repair_order_query)
        if results is None:
            return []
        inserted_op_code_ids = [x[0] for x in results.fetchall()]
        return inserted_op_code_ids

    def insert_op_codes(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into op code table and return row count."""
        desired_op_code_columns = mappings[catalog_name]["op_codes"].keys()
        actual_op_code_columns = [
            x for x in desired_op_code_columns if x in dealer_df.columns
        ]
        actual_op_code_columns.append("dealer_integration_partner_id")

        combined_op_code_df = dealer_df.select(actual_op_code_columns).withColumn(
            "op_codes", F.explode("op_codes")
        )
        unique_op_code_cols = [
            "dealer_integration_partner_id",
            "op_code",
            "op_code_desc",
        ]
        op_code_df = combined_op_code_df.select(
            F.col("op_codes.op_code").alias("op_code"),
            F.col("op_codes.op_code_desc").alias("op_code_desc"),
            "dealer_integration_partner_id",
        ).dropDuplicates(subset=unique_op_code_cols)
        insert_op_code_query = self.get_insert_query_from_df(
            op_code_df,
            "op_code",
            f"""ON CONFLICT ON CONSTRAINT unique_op_code DO UPDATE
            SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, op_code.{x})' for x in op_code_df.columns])}
            RETURNING id""",
        )

        results = self.commit_rds(insert_op_code_query)
        if results is None:
            return []
        inserted_op_code_ids = [x[0] for x in results.fetchall()]
        return inserted_op_code_ids

    def insert_service_repair_order(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into service repair order table and return row count."""
        desired_service_repair_order_columns = mappings[catalog_name][
            "service_repair_order"
        ].keys()
        actual_service_repair_order_columns = [
            x for x in desired_service_repair_order_columns if x in dealer_df.columns
        ]
        actual_service_repair_order_columns.append("dealer_integration_partner_id")
        actual_service_repair_order_columns.append("consumer_id")
        actual_service_repair_order_columns.append("vehicle_id")
        actual_service_repair_order_columns.append("metadata")

        unique_ros_dms_cols = ["repair_order_no", "dealer_integration_partner_id"]
        service_repair_order_df = dealer_df.select(
            actual_service_repair_order_columns
        ).dropDuplicates(subset=unique_ros_dms_cols)
        insert_service_repair_order_query = self.get_insert_query_from_df(
            service_repair_order_df,
            "service_repair_order",
            f"""ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
            SET {', '.join([f'{x} = COALESCE(EXCLUDED.{x}, service_repair_order.{x})' for x in service_repair_order_df.columns])}
            RETURNING id""",
        )

        results = self.commit_rds(insert_service_repair_order_query)
        if results is None:
            return []
        inserted_service_repair_order_ids = [x[0] for x in results.fetchall()]
        return inserted_service_repair_order_ids

    def insert_vehicle_sale(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into vehicle sale table and return row count."""
        desired_vehicle_sale_columns = mappings[catalog_name]["vehicle_sale"].keys()
        actual_vehicle_sale_columns = [
            x for x in desired_vehicle_sale_columns if x in dealer_df.columns
        ]
        actual_vehicle_sale_columns.append("dealer_integration_partner_id")
        actual_vehicle_sale_columns.append("consumer_id")
        actual_vehicle_sale_columns.append("vehicle_id")
        actual_vehicle_sale_columns.append("metadata")

        vehicle_sale_df = dealer_df.select(actual_vehicle_sale_columns)
        insert_vehicle_sale_query = self.get_insert_query_from_df(
            vehicle_sale_df,
            "vehicle_sale",
            "ON CONFLICT ON CONSTRAINT unique_vehicle_sale DO NOTHING RETURNING id",
        )

        results = self.commit_rds(insert_vehicle_sale_query)
        if results is None:
            return []
        inserted_vehicle_sale_ids = [x[0] for x in results.fetchall()]
        return inserted_vehicle_sale_ids

    def insert_consumer(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into consumer table and return created ids."""
        desired_consumer_columns = mappings[catalog_name]["consumer"].keys()
        actual_consumer_columns = [
            x for x in desired_consumer_columns if x in dealer_df.columns
        ]
        actual_consumer_columns.append("dealer_integration_partner_id")
        actual_consumer_columns.append("metadata")

        consumer_df = dealer_df.select(actual_consumer_columns)
        insert_consumer_query = self.get_insert_query_from_df(
            consumer_df, "consumer", "RETURNING id"
        )

        results = self.commit_rds(insert_consumer_query)
        if results is None:
            return []
        inserted_consumer_ids = [x[0] for x in results.fetchall()]
        return inserted_consumer_ids

    def insert_vehicle(self, dealer_df, catalog_name, mappings):
        """Given a dataframe of dealer data insert into vehicle table and return created ids."""
        desired_vehicle_columns = mappings[catalog_name]["vehicle"].keys()
        actual_vehicle_columns = [
            x for x in desired_vehicle_columns if x in dealer_df.columns
        ]
        actual_vehicle_columns.append("dealer_integration_partner_id")
        actual_vehicle_columns.append("metadata")

        vehicle_df = dealer_df.select(actual_vehicle_columns)
        insert_vehicle_query = self.get_insert_query_from_df(
            vehicle_df, "vehicle", "RETURNING id"
        )

        results = self.commit_rds(insert_vehicle_query)
        if results is None:
            return []
        inserted_vehicle_ids = [x[0] for x in results.fetchall()]
        return inserted_vehicle_ids


class DynamicFrameResolver:
    """Resolve choices from a glue dynamic frame."""

    def __init__(self, glue_context):
        self.glue_context = glue_context

    def get_resolved_choices(self, unresolved_columns):
        """Determine how to handle unresolved columns"""
        resolve_spec = []
        for unresolved_column in unresolved_columns:
            choices = unresolved_column[1].keys()
            field_name = unresolved_column[0]
            if "struct" in choices and "array" in choices:
                resolve_spec.append((field_name, "make_cols"))
            else:
                resolve_spec.append((field_name, "cast:string"))
        return resolve_spec

    def get_unresolved_columns(self, schema):
        """Check dynamic frame for unresolved data types."""

        def _recursion(field_map, unresolved_column_names, build_name=""):
            for field in field_map.values():
                field_name = f"{build_name + '.' if build_name else ''}{field.name if field.name else ''}"
                if isinstance(field.dataType, StructType):
                    _recursion(
                        field.dataType.field_map, unresolved_column_names, field_name
                    )
                if isinstance(field.dataType, ArrayType):
                    if not isinstance(field.dataType.elementType, NullType):
                        if isinstance(field.dataType.elementType, StructType):
                            field_name += "[]"
                            _recursion(
                                field.dataType.elementType.field_map,
                                unresolved_column_names,
                                field_name,
                            )
                        if isinstance(field.dataType.elementType, ChoiceType):
                            unresolved_column_names.append(
                                (field_name, field.dataType.elementType.choices)
                            )
                if isinstance(field.dataType, ChoiceType):
                    unresolved_column_names.append((field_name, field.dataType.choices))

        unresolved_columns = []
        _recursion(schema.field_map, unresolved_columns)
        # Sort most nested to least nested
        unresolved_columns = sorted(
            unresolved_columns, key=lambda x: len(x[0].split(".")), reverse=False
        )
        return unresolved_columns

    def replace_struct(
        self, df, source_col_name, full_split_field_rp, replaced_fields=[]
    ):
        """For a given dataframe, replace fields inside a nested struct with a new field containing data from a source column."""

        def recursive_struct_update(source_col_name, full_split_field_rp, index=0):
            if len(full_split_field_rp) == index + 1:
                # This struct field is getting replaced with the source column data
                return F.col(source_col_name).alias(full_split_field_rp[-1])
            else:
                current_prefix = ".".join(full_split_field_rp[: index + 1])
                current_field = full_split_field_rp[index + 1]
                if len(full_split_field_rp) == index + 2:
                    # The next struct field is getting replaced, preserve all struct fields except the replaced fields
                    return (
                        F.struct(
                            F.col(f"{current_prefix}.*"),
                            recursive_struct_update(
                                source_col_name, full_split_field_rp, index=index + 1
                            ),
                        )
                        .alias(current_field)
                        .dropFields(*replaced_fields)
                    )
                else:
                    # Preserve all fields in the current struct
                    return F.struct(
                        F.col(f"{current_prefix}.*"),
                        recursive_struct_update(
                            source_col_name, full_split_field_rp, index=index + 1
                        ).alias(current_field),
                    )

        return df.withColumn(
            full_split_field_rp[0],
            recursive_struct_update(source_col_name, full_split_field_rp),
        ).drop(source_col_name)

    def convert_structs_to_arrays(self, df, spec):
        """Given a dynamicframe and a spec, resolve any split struct/array columns by combining them into an array."""
        spec = sorted(spec, key=lambda x: len(x[0].split(".")), reverse=False)
        for field_path, resolution in spec:
            if resolution != "make_cols":
                continue

            field_path = (
                field_path.replace("[]", "")
                .replace("_struct", "")
                .replace("_array", "")
            )
            temp_column_name = "temp"
            struct_field = f"{field_path}_struct"
            array_field = f"{field_path}_array"
            temp_array_column = f"{temp_column_name}_array"
            temp_struct_column = f"{temp_column_name}_struct"

            combine_columns = (
                lambda row: row[temp_array_column]
                if row[temp_array_column] is not None
                else row[temp_struct_column]
                if row[temp_struct_column] is not None
                else None
            )

            df = (
                df.withColumn(
                    # Move nested array data to a temp new column
                    temp_array_column,
                    F.col(array_field),
                )
                .withColumn(
                    # Move the nested struct data to a temp new column converted to an array
                    temp_struct_column,
                    F.array(F.col(struct_field)),
                )
                .withColumn(
                    # Combine the two temp columns to a new column
                    temp_column_name,
                    combine_columns(df),
                )
                .drop(
                    # Remove the temp array column
                    temp_array_column
                )
                .drop(
                    # Remove the temp struct column
                    temp_struct_column
                )
            )

            # With the new column of combined array data, replace the original nested data
            df = (
                self.replace_struct(
                    df,
                    temp_column_name,
                    field_path.split("."),
                    replaced_fields=[
                        array_field.split(".")[-1],
                        struct_field.split(".")[-1],
                    ],
                )
                .drop(struct_field)
                .drop(array_field)
            )
        return df

    def resolve_nested(self, df, name):
        """Resolve nested choices inside a dynamicframe."""
        i = 0
        max_resolutions = 50
        all_resolved_specs = []
        unresolved_columns = self.get_unresolved_columns(df.schema())
        while len(unresolved_columns) > 0:
            if i > max_resolutions:
                raise RuntimeError(
                    f"Unable to resolve {name} with {unresolved_columns}"
                )
            resolved_spec = self.get_resolved_choices(unresolved_columns)
            all_resolved_specs += resolved_spec
            df = df.resolveChoice(specs=resolved_spec)
            unresolved_columns = self.get_unresolved_columns(df.schema())
            i += 1
        return df, all_resolved_specs

    def resolve_reyrey_df(self, datasource, main_column_name):
        """
        XML data makes no indication of arrays vs structs
        This causes the crawler to make choice columns where the column can be either an array or struct
        For each field where this occurs we want to convert it to be an array of structs
        The main data column is the exception where we instead convert arrays to structs where each row is then a struct
        Note we unfortunately can't use rowTag for the main data column because we would lose the ApplicationArea tag
        """
        array_column_name = f"{main_column_name}_array"
        struct_column_name = f"{main_column_name}_struct"

        # Split main data column into array and struct column
        datasource = datasource.resolveChoice(specs=(main_column_name, "make_cols"))

        # Separate array and struct columns into different dataframes where they aren't null.
        datasource_array = (
            datasource.withColumnRenamed(array_column_name, main_column_name)
            .filter(lambda row: row[main_column_name] is not None)
            .drop(struct_column_name)
            .drop("RepairOrder.RoRecord.Rosub")
            .drop("RepairOrder.RoRecord.Ropart")
            .drop("RepairOrder.RoRecord.Rolabor.OpCodeLaborInfo")
        )
        datasource_struct = (
            datasource.withColumnRenamed(struct_column_name, main_column_name)
            .filter(lambda row: row[main_column_name] is not None)
            .drop(array_column_name)
            .drop("RepairOrder.RoRecord.Rosub")
            .drop("RepairOrder.RoRecord.Ropart")
            .drop("RepairOrder.RoRecord.Rolabor.OpCodeLaborInfo")
        )

        # Resolve all possible field choices inside the frame and get the resolved choices
        datasource_array, array_columns_spec = self.resolve_nested(
            datasource_array, array_column_name
        )
        datasource_struct, struct_columns_spec = self.resolve_nested(
            datasource_struct, struct_column_name
        )

        # Convert the array data into rows of structs
        datasource_array = datasource_array.withColumn(
            main_column_name, F.explode(F.col(main_column_name))
        )
        datasource_array, array_columns_spec_nested = self.resolve_nested(
            datasource_array, main_column_name
        )
        array_columns_spec = array_columns_spec_nested + array_columns_spec

        # Cast all array/struct choices to be arrays
        datasource_array = self.convert_structs_to_arrays(
            datasource_array, array_columns_spec
        )
        datasource_struct = self.convert_structs_to_arrays(
            datasource_struct, struct_columns_spec
        )

        # Combine back into one dataframe
        datasource = datasource_array.union(datasource_struct)
        datasource, datasource_spec = self.resolve_nested(datasource, main_column_name)
        datasource = self.convert_structs_to_arrays(datasource, datasource_spec)

        return datasource


class ReyReyUpsertJob:
    """Create object to perform ETL."""

    def __init__(self, job_id, args):
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(args["JOB_NAME"], args)
        self.job_id = job_id
        self.catalog_table_names = args["catalog_table_names"].split(",")
        self.temp_dir = args["TempDir"]
        self.dlq_url = args["dlq_url"]
        self.database = args["db_name"]
        self.region = args["region"]
        self.is_prod = args["environment"] == "prod"
        self.integration = "reyrey"
        self.bucket_name = (
            f"integrations-us-east-1-{'prod' if self.is_prod else 'test'}"
        )
        self.rds = RDSInstance(self.is_prod, self.integration)
        self.resolver = DynamicFrameResolver(self.glue_context)
        self.required_columns = []
        self.mappings = {
            "reyreycrawlerdb_fi_closed_deal": {
                "dealer": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "consumer": {
                    "dealer_customer_no": "FIDeal.Buyer.CustRecord.ContactInfo._NameRecId",
                    "first_name": "FIDeal.Buyer.CustRecord.ContactInfo._FirstName",
                    "last_name": "FIDeal.Buyer.CustRecord.ContactInfo._LastName",
                    "email": "FIDeal.Buyer.CustRecord.ContactInfo.Email._MailTo",
                    "cell_phone": "FIDeal.Buyer.CustRecord.ContactInfo.phone",
                    "postal_code": "FIDeal.Buyer.CustRecord.ContactInfo.Address._Zip",
                    "home_phone": "FIDeal.Buyer.CustRecord.ContactInfo.phone",
                    "email_optin_flag": "FIDeal.Buyer.CustRecord.CustPersonal._OptOut",
                    "phone_optin_flag": "FIDeal.Buyer.CustRecord.CustPersonal._OptOut",
                    "postal_mail_optin_flag": "FIDeal.Buyer.CustRecord.CustPersonal._OptOut",
                    "sms_optin_flag": "FIDeal.Buyer.CustRecord.CustPersonal._OptOut",
                },
                "vehicle": {
                    "new_or_used": "FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._NewUsed",
                    "vin": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin",
                    "model": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._ModelDesc",
                    "year": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr",
                },
                "vehicle_sale": {
                    "sale_date": "FIDeal.FIDealFin._CloseDealDate",
                    "listed_price": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
                    "mileage_on_vehicle": "FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._OdomReading",
                    "deal_type": "FIDeal.FIDealFin._Category",
                    "cost_of_vehicle": "FIDeal.FIDealFin.TransactionVehicle._VehCost",
                    "oem_msrp": "FIDeal.FIDealFin.TransactionVehicle._MSRP",
                    "adjustment_on_price": "FIDeal.FIDealFin.TransactionVehicle._Discount",
                    "days_in_stock": "FIDeal.FIDealFin.TransactionVehicle._DaysInStock",
                    "date_of_state_inspection": "FIDeal.FIDealFin.TransactionVehicle._InspectionDate",
                    "trade_in_value": "FIDeal.FIDealFin.TradeIn._ActualCashValue",
                    "payoff_on_trade": "FIDeal.FIDealFin.TradeIn._Payoff",
                    "value_at_end_of_lease": "FIDeal.FIDealFin.FinanceInfo.LeaseSpec._VehicleResidual",
                    "miles_per_year": "FIDeal.FIDealFin.FinanceInfo.LeaseSpec._EstDrvYear",
                    # TODO: Moved to service_contract
                    # "service_package_flag": "FIDeal.FIDealFin.WarrantyInfo.ServiceCont._ServContYN",
                    "vehicle_gross": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
                    # TODO: Moved to service_contract
                    # "warranty_expiration_date": "FIDeal.FIDealFin.WarrantyInfo.ExtWarranty.VehExtWarranty._ExpirationDate",
                    "delivery_date": "FIDeal.FIDealFin._DeliveryDate",
                    "vin": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin",
                    "has_service_contract": "FIDeal.FIDealFin.WarrantyInfo.ServiceCont._ServContYN",
                    "finance_rate": "FIDeal.FIDealFin.FinanceInfo._EnteredRate",
                    "finance_term": "FIDeal.FIDealFin.FinanceInfo._Term",
                    "finance_amount": "FIDeal.FIDealFin.FinanceInfo._AmtFinanced",
                },
            },
            "reyreycrawlerdb_repair_order": {
                "dealer": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "op_codes": {
                    "repair_order_no": "RepairOrder.RoRecord.Rogen._RoNo",
                    "op_codes": "RepairOrder.RoRecord.Rogen.RecommendedServc",
                },
                "consumer": {
                    "dealer_customer_no": "RepairOrder.CustRecord.ContactInfo._NameRecId",
                    "first_name": "RepairOrder.CustRecord.ContactInfo._FirstName",
                    "last_name": "RepairOrder.CustRecord.ContactInfo._LastName",
                    "email": "RepairOrder.CustRecord.ContactInfo.Email._MailTo",
                    "cell_phone": "RepairOrder.CustRecord.ContactInfo.phone",
                    "postal_code": "RepairOrder.CustRecord.ContactInfo.Address._Zip",
                    "home_phone": "RepairOrder.CustRecord.ContactInfo.phone",
                },
                "vehicle": {
                    "vin": "RepairOrder.RoRecord.Rogen._Vin",
                    "make": "RepairOrder.ServVehicle.Vehicle._VehicleMake",
                    "model": "RepairOrder.ServVehicle.Vehicle._ModelDesc",
                    "year": "RepairOrder.ServVehicle.Vehicle._VehicleYr",
                },
                "service_repair_order": {
                    "ro_open_date": "RepairOrder.RoRecord.Rogen._RoCreateDate",
                    "ro_close_date": "RepairOrder.ServVehicle.VehicleServInfo._LastRODate",
                    "txn_pay_type": "RepairOrder.RoRecord.Rolabor.RoAmts._PayType",
                    "repair_order_no": "RepairOrder.RoRecord.Rogen._RoNo",
                    "advisor_name": "RepairOrder.RoRecord.Rogen._AdvName",
                    # Total amount will be the sum of internal, consumer, and warranty at the transform stage.
                    "total_amount": "RepairOrder.RoRecord.Rogen._IntrRoTotalAmt",
                    "internal_total_amount": "RepairOrder.RoRecord.Rogen._IntrRoTotalAmt",
                    "consumer_total_amount": "RepairOrder.RoRecord.Rogen._CustRoTotalAmt",
                    "warranty_total_amount": "RepairOrder.RoRecord.Rogen._WarrRoTotalAmt",
                    "comment": "RepairOrder.RoRecord.Rogen.RoCommentInfo._RoComment",
                    "recommendation": "RepairOrder.RoRecord.Rogen.TechRecommends._TechRecommend",
                },
            },
        }

    def apply_mappings(self, df, table_to_mappings):
        """Select valid db columns from a dataframe using dms column mappings, log and skip missing data."""
        ignore_columns = []
        selected_columns = []
        selected_column_names = []
        for db_columns_to_dms_columns in table_to_mappings.values():
            for db_column, dms_column in db_columns_to_dms_columns.items():
                try:
                    df.select(dms_column)
                except pyspark.sql.utils.AnalysisException:
                    ignore_columns.append(db_column)
                    logger.exception(
                        f"Column: {db_column} with mapping: {dms_column} not found in schema {df.schema.json()}."
                    )
                    if db_column in self.required_columns:
                        raise

            for db_column, dms_column in db_columns_to_dms_columns.items():
                if (
                    db_column not in ignore_columns
                    and db_column not in selected_column_names
                ):
                    selected_columns.append(F.col(dms_column).alias(db_column))
                    selected_column_names.append(db_column)
        # Additional data not directly mapped to table
        df = df.withColumn("s3_url", F.input_file_name())
        df = df.withColumn("Region", F.lit(self.region))
        df = df.withColumn("BODId", F.col("ApplicationArea.BODId"))
        df = df.withColumn("metadata", F.lit(None))
        selected_columns += [
            "s3_url",
            "Region",
            "BODId",
            "metadata",
            "PartitionYear",
            "PartitionMonth",
            "PartitionDate",
        ]
        return df.select(selected_columns)

    def format_df(self, df, catalog_name):
        """Format the raw data to match the database schema."""
        if "postal_code" in df.columns:
            # Convert from Array to String
            def calculate_postal_code(arr):
                if arr:
                    if not isinstance(arr, list):
                        arr = [arr]
                    for raw_reyrey_str in arr:
                        if raw_reyrey_str and raw_reyrey_str != "null":
                            return raw_reyrey_str
                    return None
                else:
                    return None

            get_postal_code = F.udf(calculate_postal_code, StringType())
            df = df.withColumn("postal_code", get_postal_code(F.col("postal_code")))
        if "cell_phone" in df.columns:
            # Convert cell_phone column from Array[Struct(_Num, _Type)] to String
            get_cell_phone = F.filter(
                F.col("cell_phone"), lambda x: x["_Type"].isin(["C", "O"])
            )["_Num"][0]
            df = df.withColumn("cell_phone", get_cell_phone)
        if "home_phone" in df.columns:
            # Convert home_phone column from Array[Struct(_Num, _Type)] to LongType
            get_home_phone = F.filter(
                F.col("home_phone"), lambda x: x["_Type"].isin(["H"])
            )["_Num"][0]
            df = df.withColumn("home_phone", get_home_phone)
        if (
            "internal_total_amount" in df.columns
            or "consumer_total_amount" in df.columns
            or "warranty_total_amount" in df.columns
        ):
            # Sum _IntrRoTotalAmt, _CustRoTotalAmt, and _WarrRoTotalAmt to get total_amount
            valid_columns = []
            if "internal_total_amount" in df.columns:
                valid_columns.append(F.col("internal_total_amount"))
            if "consumer_total_amount" in df.columns:
                valid_columns.append(F.col("consumer_total_amount"))
            if "warranty_total_amount" in df.columns:
                valid_columns.append(F.col("warranty_total_amount"))

            df = df.withColumn(
                "total_amount",
                F.when(
                    # When all the columns are null, keep the value null
                    F.coalesce(*valid_columns).isNull(),
                    F.lit(None),
                ).otherwise(
                    # When at least one column has a value, treat null values as 0 and sum
                    sum([F.coalesce(x, F.lit(0)) for x in valid_columns])
                ),
            )
        if "op_codes" in df.columns:
            # Convert op_code column from Array[Struct(_RecSvcOpCdDesc, _RecSvcOpCode)] to Array[Struct(op_code_desc, op_code)]
            df = df.withColumn(
                "op_codes",
                F.expr(
                    "transform(op_codes, x -> struct(x._RecSvcOpCode as op_code, x._RecSvcOpCdDesc as op_code_desc))"
                ),
            )
        if "txn_pay_type" in df.columns:
            # Convert Array[String] to String
            df = df.withColumn("txn_pay_type", F.concat_ws(",", F.col("txn_pay_type")))
        if "has_service_contract" in df.columns:
            # Convert String to Bool
            def calculate_service_contract_flag(arr):
                if arr:
                    if not isinstance(arr, list):
                        arr = [arr]
                    return any(x == "Y" for x in arr)
                else:
                    return None

            get_service_contract_flag = F.udf(
                calculate_service_contract_flag, BooleanType()
            )
            df = df.withColumn(
                "has_service_contract",
                get_service_contract_flag(F.col("has_service_contract")),
            )
        if ("trade_in_value" in df.columns
            or "payoff_on_trade" in df.columns):
            # Convert Array[Float] to Float
            def calculate_arr_sum(arr):
                if arr:
                    if not isinstance(arr, list):
                        arr = [arr]
                    total = None
                    for raw_reyrey_float in arr:
                        if raw_reyrey_float and raw_reyrey_float != "null":
                            if total is None:
                                total = raw_reyrey_float
                            else:
                                total += raw_reyrey_float
                    return total
                else:
                    return None
            if "trade_in_value" in df.columns:
                get_trade_in_value = F.udf(calculate_arr_sum, DoubleType())
                df = df.withColumn("trade_in_value", get_trade_in_value(F.col("trade_in_value")))
            if "payoff_on_trade" in df.columns:
                get_payoff_on_trade = F.udf(calculate_arr_sum, DoubleType())
                df = df.withColumn("payoff_on_trade", get_payoff_on_trade(F.col("payoff_on_trade")))
        if (
            "email_optin_flag" in df.columns
            and "phone_optin_flag" in df.columns
            and "postal_mail_optin_flag" in df.columns
            and "sms_optin_flag" in df.columns
        ):
            # Convert String to Bool
            def calculate_optin_flag(arr):
                if arr:
                    if not isinstance(arr, list):
                        arr = [arr]
                    # Optin False if any optout is Y
                    return not any(x == "Y" for x in arr)
                else:
                    return None

            get_optin_flag = F.udf(calculate_optin_flag, BooleanType())
            # Same flag for each, copy rather than recalculate
            df = (
                df.withColumn(
                    "email_optin_flag", get_optin_flag(F.col("email_optin_flag"))
                )
                .withColumn("phone_optin_flag", F.col("email_optin_flag"))
                .withColumn("postal_mail_optin_flag", F.col("email_optin_flag"))
                .withColumn("sms_optin_flag", F.col("email_optin_flag"))
            )
        if "metadata" in df.columns:
            df = df.withColumn(
                "metadata",
                F.to_json(
                    F.struct(
                        "Region",
                        "PartitionYear",
                        "PartitionMonth",
                        "PartitionDate",
                        "BODId",
                        "s3_url",
                    )
                ),
            )

        return df

    def add_list_to_df(
        self, df, add_list, add_list_column_name, temp_col_name="order_temp"
    ):
        """Given a dataframe and a list, add the list as a column to the dataframe preserving order."""
        w = Window.partitionBy(F.lit(1)).orderBy(F.lit(1))
        temp_df = self.spark.createDataFrame(
            [[x] for x in add_list], [add_list_column_name]
        ).withColumn(temp_col_name, F.row_number().over(w))
        df = (
            df.withColumn(temp_col_name, F.row_number().over(w))
            .join(temp_df, [temp_col_name])
            .drop(F.col(temp_col_name))
        )
        return df

    def upsert_df(self, df, catalog_name):
        """Upsert dataframe to RDS table."""
        insert_count = 0
        current_dealer = None
        dealers = df.select("dms_id").distinct().collect()
        if len(dealers) == 0:
            logger.error("No data found for any dealer")
        for dealer in dealers:
            try:
                current_dealer = dealer.dms_id
                dealer_df = df.filter(df.dms_id == dealer.dms_id)
                db_dealer_integration_partner_id = None

                db_dealer_integration_partner_id = (
                    self.rds.select_db_dealer_integration_partner_id(dealer.dms_id)
                )
                dealer_df = dealer_df.withColumn(
                    "dealer_integration_partner_id",
                    F.lit(db_dealer_integration_partner_id),
                )
                dealer_rows = dealer_df.count()
                if catalog_name == "reyreycrawlerdb_fi_closed_deal":
                    # Vehicle sale must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    if count != dealer_rows:
                        raise RuntimeError(
                            f"Unable to insert consumers, expected {dealer_rows} got {count}"
                        )
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_integration_partner_id}"
                    )
                    vehicle_sale_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )

                    # Then insert into vehicle
                    inserted_vehicle_ids = self.rds.insert_vehicle(
                        vehicle_sale_df, catalog_name, self.mappings
                    )
                    count = len(inserted_vehicle_ids)
                    if count != dealer_rows:
                        raise RuntimeError(
                            f"Unable to insert vehicles, expected {dealer_rows} got {count}"
                        )
                    logger.info(
                        f"Added {count} rows to vehicle for dealer {db_dealer_integration_partner_id}"
                    )
                    vehicle_sale_df = self.add_list_to_df(
                        vehicle_sale_df, inserted_vehicle_ids, "vehicle_id"
                    )

                    # Then insert into vehicle sale
                    vehicle_sale_ids = self.rds.insert_vehicle_sale(
                        vehicle_sale_df, catalog_name, self.mappings
                    )
                    count = len(vehicle_sale_ids)
                    logger.info(
                        f"Added {count} rows to vehicle_sale for dealer {db_dealer_integration_partner_id}"
                    )
                    insert_count += count

                elif catalog_name == "reyreycrawlerdb_repair_order":
                    # Service repair order must insert into consumer table first
                    inserted_consumer_ids = self.rds.insert_consumer(
                        dealer_df, catalog_name, self.mappings
                    )
                    count = len(inserted_consumer_ids)
                    logger.info(
                        f"Added {count} rows to consumer for dealer {db_dealer_integration_partner_id}"
                    )
                    service_repair_order_df = self.add_list_to_df(
                        dealer_df, inserted_consumer_ids, "consumer_id"
                    )

                    # Then insert into vehicle
                    inserted_vehicle_ids = self.rds.insert_vehicle(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(inserted_vehicle_ids)
                    if count != dealer_rows:
                        raise RuntimeError(
                            f"Unable to insert vehicles, expected {dealer_rows} got {count}"
                        )
                    logger.info(
                        f"Added {count} rows to vehicle for dealer {db_dealer_integration_partner_id}"
                    )
                    service_repair_order_df = self.add_list_to_df(
                        service_repair_order_df, inserted_vehicle_ids, "vehicle_id"
                    )

                    # Then insert into service repair orders
                    service_repair_order_ids = self.rds.insert_service_repair_order(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(service_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to service_repair_order for dealer {db_dealer_integration_partner_id}"
                    )
                    insert_count += count

                    # Create op codes
                    inserted_op_code_ids = self.rds.insert_op_codes(
                        service_repair_order_df, catalog_name, self.mappings
                    )
                    count = len(inserted_op_code_ids)
                    logger.info(
                        f"Added {count} rows to op_code for dealer {db_dealer_integration_partner_id}"
                    )

                    # Link op codes
                    inserted_op_code_repair_order_ids = (
                        self.rds.insert_op_code_repair_order(service_repair_order_df)
                    )
                    count = len(inserted_op_code_repair_order_ids)
                    logger.info(
                        f"Added {count} rows to op_code_repair_order for dealer {db_dealer_integration_partner_id}"
                    )
            except Exception:
                s3_key = f"{self.integration}/errors/{datetime.now().strftime('%Y-%m-%d')}/{self.job_id}/{uuid.uuid4().hex}.json"
                logger.exception(
                    f"Error inserting {catalog_name} for dealer {dealer_df} save data {s3_key}"
                )
                self.save_df_notify(dealer_df, s3_key)
        return insert_count

    def save_df_notify(self, df, s3_key):
        """Save schema and data to s3, notify of error."""
        schema_json = loads(df.schema.json())
        data_json = [row.asDict(recursive=True) for row in df.collect()]
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=dumps({"schema": schema_json, "data": data_json}),
            ContentType="application/json",
        )
        logger.info(f"Uploaded df info to {s3_key}")
        sqs_client = boto3.client("sqs")
        sqs_client.send_message(
            QueueUrl=self.dlq_url,
            MessageBody=dumps({"bucket": self.bucket_name, "key": s3_key}),
        )

    def run(self):
        """Run ETL for each table in our catalog."""
        for catalog_name in self.catalog_table_names:
            if "reyreycrawlerdb_fi_closed_deal" == catalog_name:
                main_column_name = "FIDeal"
            elif "reyreycrawlerdb_repair_order" == catalog_name:
                main_column_name = "RepairOrder"
            else:
                raise RuntimeError(f"Unexpected catalog {catalog_name}")

            # Retrieve data from the glue catalog
            datasource = self.glue_context.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=catalog_name,
                transformation_ctx=f"context_{catalog_name}",
            )

            if (
                datasource.filter(lambda row: row[main_column_name] is not None).count()
                == 0
            ):
                logger.info("No new data to parse")
                return

            # Rename partitions from s3
            datasource = (
                datasource.withColumnRenamed("partition_0", "PartitionYear")
                .withColumnRenamed("partition_1", "PartitionMonth")
                .withColumnRenamed("partition_2", "PartitionDate")
            )

            # Resolve choices, flatten dataframe
            datasource = self.resolver.resolve_reyrey_df(
                datasource, main_column_name
            ).toDF()
            # Get the base fields in a df via self.mappings
            datasource = self.apply_mappings(datasource, self.mappings[catalog_name])
            # Format necessary base fields to get a standardized df format
            datasource = self.format_df(datasource, catalog_name)
            # Insert tables to database
            self.upsert_df(datasource, catalog_name)

            self.job.commit()


if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "db_name",
            "catalog_table_names",
            "catalog_connection",
            "environment",
            "dlq_url",
            "region",
        ],
    )

    job_id = args["JOB_RUN_ID"]
    logging.basicConfig(
        format="reyrey_" + str(job_id) + " %(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    try:
        job = ReyReyUpsertJob(job_id, args)
        job.run()
    except Exception:
        logger.exception("Error running ReyRey ETL.")
        raise
