"""Rey Rey ETL Job."""
import logging
import sys
from json import loads

import boto3
import psycopg2
import pyspark
import pyspark.sql.functions as F
from awsglue.context import GlueContext
from awsglue.gluetypes import ArrayType, ChoiceType, NullType, StructType
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import BooleanType, DoubleType, StringType


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

    def get_table_names(self):
        """ Get a list of table names in the database. """
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        cursor.execute(query)
        table_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return table_names

    def get_table_column_names(self, table_name):
        """ Get a list of column names in the given database table. """
        conn = self.get_rds_connection()
        cursor = conn.cursor()
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        cursor.execute(query)
        column_names = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return column_names

    def get_unified_column_names(self):
        """ Get a list of column names from all database tables in unified format. """
        unified_column_names = []
        tables = self.get_table_names()
        for table in tables:
            columns = self.get_table_column_names(table)
            for column in columns:
                unified_column_names.append(f"{table}|{column}")
        return unified_column_names


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
        logger.info(f"Choice resolution starting schema {datasource.schema()}")
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

        logger.info(f"Choice resolution ending schema {datasource.schema()}")

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
        self.metadata_tables = ["service_repair_order", "vehicle_sale", "vehicle", "consumer"]
        self.mappings = {
            "reyreycrawlerdb_fi_closed_deal": {
                "dealer_integration_partner": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
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
                    "vehicle_gross": "FIDeal.FIDealFin.Recap.Reserves._VehicleGross",
                    "delivery_date": "FIDeal.FIDealFin._DeliveryDate",
                    "vin": "FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin",
                    "has_service_contract": "FIDeal.FIDealFin.WarrantyInfo.ServiceCont._ServContYN",
                    "finance_rate": "FIDeal.FIDealFin.FinanceInfo._EnteredRate",
                    "finance_term": "FIDeal.FIDealFin.FinanceInfo._Term",
                    "finance_amount": "FIDeal.FIDealFin.FinanceInfo._AmtFinanced",
                },
            },
            "reyreycrawlerdb_repair_order": {
                "dealer_integration_partner": {"dms_id": "ApplicationArea.Sender.DealerNumber"},
                "op_codes": {
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
        for db_table_name, db_columns_to_dms_columns in table_to_mappings.items():
            for db_column, dms_column in db_columns_to_dms_columns.items():
                try:
                    df.select(dms_column)
                except pyspark.sql.utils.AnalysisException:
                    ignore_columns.append(db_column)
                    logger.warning(
                        f"Column: {db_column} with mapping: {dms_column} not found in schema {df.schema.json()}."
                    )
                    if db_column in self.required_columns:
                        raise
                else:
                    selected_columns.append(F.col(dms_column).alias(f"{db_table_name}|{db_column}"))

            if db_table_name in self.metadata_tables:
                df = df.withColumn(
                    f"{db_table_name}|metadata",
                    F.to_json(
                        F.struct(
                            F.lit(self.region).alias("Region"),
                            "PartitionYear",
                            "PartitionMonth",
                            "PartitionDate",
                            F.col("ApplicationArea.BODId").alias("BODId"),
                            F.input_file_name().alias("s3_url"),
                        )
                    ),
                )

        selected_columns += ["PartitionYear", "PartitionMonth", "PartitionDate"]

        return df.select(selected_columns)

    def format_df(self, df, catalog_name):
        """Format the raw data to match the database schema."""
        if "consumer|postal_code" in df.columns:
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
            df = df.withColumn("consumer|postal_code", get_postal_code(F.col("consumer|postal_code")))
        if "consumer|cell_phone" in df.columns:
            # Convert cell_phone column from Array[Struct(_Num, _Type)] to String
            get_cell_phone = F.filter(
                F.col("consumer|cell_phone"), lambda x: x["_Type"].isin(["C", "O"])
            )["_Num"][0]
            df = df.withColumn("consumer|cell_phone", get_cell_phone)
        if "consumer|home_phone" in df.columns:
            # Convert home_phone column from Array[Struct(_Num, _Type)] to LongType
            get_home_phone = F.filter(
                F.col("consumer|home_phone"), lambda x: x["_Type"].isin(["H"])
            )["_Num"][0]
            df = df.withColumn("consumer|home_phone", get_home_phone)
        if (
            "service_repair_order|internal_total_amount" in df.columns
            or "service_repair_order|consumer_total_amount" in df.columns
            or "service_repair_order|warranty_total_amount" in df.columns
        ):
            # Sum _IntrRoTotalAmt, _CustRoTotalAmt, and _WarrRoTotalAmt to get total_amount
            valid_columns = []
            if "service_repair_order|internal_total_amount" in df.columns:
                valid_columns.append(F.col("service_repair_order|internal_total_amount"))
            if "service_repair_order|consumer_total_amount" in df.columns:
                valid_columns.append(F.col("service_repair_order|consumer_total_amount"))
            if "service_repair_order|warranty_total_amount" in df.columns:
                valid_columns.append(F.col("service_repair_order|warranty_total_amount"))

            df = df.withColumn(
                "service_repair_order|total_amount",
                F.when(
                    # When all the columns are null, keep the value null
                    F.coalesce(*valid_columns).isNull(),
                    F.lit(None),
                ).otherwise(
                    # When at least one column has a value, treat null values as 0 and sum
                    sum([F.coalesce(x, F.lit(0)) for x in valid_columns])
                ),
            )
        if "op_codes|op_codes" in df.columns:
            # Convert op_code column from Array[Struct(_RecSvcOpCdDesc, _RecSvcOpCode)] to Array[Struct(op_code_desc, op_code)]
            is_array_col = df.schema["op_codes|op_codes"].dataType.typeName() == "array"
            if not is_array_col:
                # XML data pulls with a single op_code aren't converted to arrays
                logger.info("Convert op_codes to array")
                df = df.withColumn(
                    "op_codes|op_codes",
                    F.array(F.struct(F.col("op_codes|op_codes.*")))
                )
            df = df.withColumn(
                "op_codes|op_codes",
                F.expr(
                    "transform(`op_codes|op_codes`, x -> struct(x._RecSvcOpCode as op_code, x._RecSvcOpCdDesc as op_code_desc))"
                ),
            )
        if "service_repair_order|txn_pay_type" in df.columns:
            # Convert Array[String] to String
            df = df.withColumn("service_repair_order|txn_pay_type", F.concat_ws(",", F.col("service_repair_order|txn_pay_type")))
        if "vehicle_sale|has_service_contract" in df.columns:
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
                "vehicle_sale|has_service_contract",
                get_service_contract_flag(F.col("vehicle_sale|has_service_contract")),
            )
        if ("vehicle_sale|trade_in_value" in df.columns
            or "vehicle_sale|payoff_on_trade" in df.columns):
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
            if "vehicle_sale|trade_in_value" in df.columns:
                get_trade_in_value = F.udf(calculate_arr_sum, DoubleType())
                df = df.withColumn("vehicle_sale|trade_in_value", get_trade_in_value(F.col("vehicle_sale|trade_in_value")))
            if "vehicle_sale|payoff_on_trade" in df.columns:
                get_payoff_on_trade = F.udf(calculate_arr_sum, DoubleType())
                df = df.withColumn("vehicle_sale|payoff_on_trade", get_payoff_on_trade(F.col("vehicle_sale|payoff_on_trade")))
        if (
            "consumer|email_optin_flag" in df.columns
            and "consumer|phone_optin_flag" in df.columns
            and "consumer|postal_mail_optin_flag" in df.columns
            and "consumer|sms_optin_flag" in df.columns
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
                    "consumer|email_optin_flag", get_optin_flag(F.col("consumer|email_optin_flag"))
                )
                .withColumn("consumer|phone_optin_flag", F.col("consumer|email_optin_flag"))
                .withColumn("consumer|postal_mail_optin_flag", F.col("consumer|email_optin_flag"))
                .withColumn("consumer|sms_optin_flag", F.col("consumer|email_optin_flag"))
            )

        logger.info(f"Format df ending schema {df.schema.json()}")
        return df

    def validate_fields(self, df):
        """ Check that each df column names matches the database. """
        # Ignore op codes (many to many array relationship) and partition columns
        ignore_table_names = ["op_codes", "PartitionYear", "PartitionMonth", "PartitionDate"]
        unified_column_names = self.rds.get_unified_column_names()
        for df_col in df.columns:
            if df_col.split("|")[0] not in ignore_table_names and df_col not in unified_column_names:
                raise RuntimeError(f"Column {df_col} not found in database {unified_column_names}")

    def run(self):
        """Run ETL for each table in our catalog."""
        for catalog_name in self.catalog_table_names:
            if "reyreycrawlerdb_fi_closed_deal" == catalog_name:
                s3_key_path = "fi_closed_deal"
                main_column_name = "FIDeal"
            elif "reyreycrawlerdb_repair_order" == catalog_name:
                s3_key_path = "repair_order"
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

            # Resolve dynamicframe choices
            datasource = self.resolver.resolve_reyrey_df(
                datasource, main_column_name
            ).toDF()
            # Get the raw fields in a df via self.mappings
            datasource = self.apply_mappings(datasource, self.mappings[catalog_name])
            # Format necessary base fields to get a standardized df format
            datasource = self.format_df(datasource, catalog_name)
            # Validate dataframe fields meet standardized df format
            self.validate_fields(datasource)
            # Write to S3
            s3_path = f"s3a://{self.bucket_name}/unified/{s3_key_path}/reyrey/"
            datasource.write.partitionBy(
                "dealer_integration_partner|dms_id",
                "PartitionYear",
                "PartitionMonth",
                "PartitionDate"
            ).mode(
                "append"
            ).parquet(
                s3_path
            )
            logger.info(f"Uploaded to {s3_path}")

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
