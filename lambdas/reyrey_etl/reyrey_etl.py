"""Rey Rey ETL Job."""

from awsglue.transforms import RenameField, Relationalize, ApplyMapping
from awsglue.utils import getResolvedOptions
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame 
import datetime
from json import dumps, loads
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, flatten, explode
import psycopg2.pool
import sys


def _load_secret():
    """Get DMS DB configuration from Secrets Manager."""

    secret_string = loads(SM_CLIENT.get_secret_value(
        SecretId='prod/DMSDB' if isprod else 'stage/DMSDB' 
    )['SecretString'])
    DB_CONFIG = {
        'db_name': secret_string['db_name'],
        'host': secret_string['host'],
        'user': secret_string['user'],
        'password': secret_string['password'],
        'port': '5432'
    }
    return DB_CONFIG


class ReyReyUpsertJob:
    """Create object to perform ETL."""


    def __init__(self, args, pool):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(args['JOB_NAME'], args)
        self.catalog_table_names = args['catalog_table_names'].split(',')
        self.upsert_table_order = self.get_upsert_table_order()
        self.pool = pool


    def get_upsert_table_order(self):
        """Return a list of tables to upsert by order of dependency."""
        upsert_table_order = {}
        for name in self.catalog_table_names:
            if 'fi_closed_deal' in name:
                upsert_table_order[name] = ['dealer', 'consumer', 'vehicle_sale']
            elif 'repair_order' in name:
                upsert_table_order[name] = ['dealer', 'consumer', 'service_repair_order', 'op_code']

        return upsert_table_order


    def apply_mappings(self, df,  tablename, catalog_table):
        """Apply appropriate mapping to dynamic frame."""
        mapping = []
        current_df = df.toDF()
        dealer_number = current_df.select("ApplicationArea.Sender.DealerNumber").collect()[0][0]

        if 'fi_closed_deal' in catalog_table:
            if tablename == 'dealer':
                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")                        
            elif tablename == 'consumer':
                current_df = current_df.select(
                    explode("FIDeal.array").alias("FIDeal")
                )
                current_df = current_df.select(
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Email").alias("email"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Address").alias("address"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.phone").alias("phone"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._NameRecId").alias("dealer_customer_no"),
                    col("FIDeal.Buyer.CustRecord.CustPersonal._OptOut").alias("opt_out_flag"),
                ).withColumn("DealerNumber", lit(dealer_number))
            elif tablename == 'vehicle_sale':
                current_df = current_df.select(
                    explode("FIDeal.array").alias("FIDeal")
                )
                current_df = current_df.select(
                    col("FIDeal.FIDealFin.TransactionVehicle._VehCost").alias("cost_of_vehicle"),
                    col("FIDeal.FIDealFin.TransactionVehicle._Discount").alias("discount_on_price"),
                    col("FIDeal.FIDealFin.TransactionVehicle._InspectionDate").alias("date_of_state_inspection"),
                    col("FIDeal.FIDealFin.TransactionVehicle._DaysInStock").alias("days_in_stock"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._Vin").alias("vin"),
                    col("FIDeal.FIDealFin.FinanceInfo.LeaseSpec._EstDrvYear").alias("miles_per_year"),
                    col("FIDeal.FIDealFin.FinanceInfo.LeaseSpec._VehicleResidual").alias("value_at_end_of_lease"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._OdomReading").alias("mileage_on_vehicle"),
                    col("FIDeal.FIDealFin.TradeIn._ActualCashValue").alias("trade_in_value"),
                    col("FIDeal.FIDealFin.TradeIn._Payoff").alias("payoff_on_trade"),
                    col("FIDeal.FIDealFin.Recap.Reserves._NetProfit").alias("profit_on_sale"),
                    col("FIDeal.FIDealFin.Recap.Reserves._VehicleGross").alias("vehicle_gross"),
                    col("FIDeal.FIDealFin.WarrantyInfo").alias("warranty_info"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._NewUsed").alias("NewUsed"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Email").alias("email"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.Address").alias("address"),
                    col("FIDeal.Buyer.CustRecord.ContactInfo.phone").alias("phone"),
                    col("FIDeal.FIDealFin.TransactionVehicle._MSRP").alias("oem_msrp"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._VehicleYr").alias("year"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail._VehClass").alias("vehicle_class"),
                    col("FIDeal.FIDealFin.TransactionVehicle.Vehicle._ModelDesc").alias("model"),
                    col("FIDeal.FIDealFin._Category").alias("deal_type"),
                    col("FIDeal.FIDealFin._CloseDealDate").alias("sale_date"),
                ).withColumn("DealerNumber", lit(dealer_number))
        elif 'repair_order' in catalog_table:
            if tablename == 'dealer':
                current_df = current_df.select("ApplicationArea.Sender.DealerNumber")                       
            elif tablename == 'op_code':
                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._RoNo").alias("repair_order_no"),
                    col("RepairOrder.RoRecord.Rogen.RecommendedServc").alias("opcodes")
                ).withColumn("DealerNumber", lit(dealer_number))
                         
            elif tablename == 'consumer':
                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )
                current_df = current_df.select(
                    col("RepairOrder.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("RepairOrder.CustRecord.ContactInfo._LastName").alias("lastname"),
                    col("RepairOrder.CustRecord.ContactInfo.phone").alias("phone"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email"),
                    col("RepairOrder.CustRecord.ContactInfo.Address").alias("address"),
                    col("RepairOrder.CustRecord.ContactInfo._NameRecId").alias("dealer_customer_no"),

                ).withColumn("DealerNumber", lit(dealer_number))
            elif tablename == 'service_repair_order':
                current_df = current_df.select(
                    explode("RepairOrder.array").alias("RepairOrder")
                )
                current_df = current_df.select(
                    col("RepairOrder.RoRecord.Rogen._Vin").alias("vin"),
                    col("RepairOrder.RoRecord.Rogen._RoNo").alias("repair_order_no"),
                    col("RepairOrder.RoRecord.Rogen._CustRoTotalAmt").alias("consumer_total_amount"),
                    col("RepairOrder.RoRecord.Rogen._RoCreateDate").alias("repair_order_open_date"),
                    col("RepairOrder.RoRecord.Rogen.TechRecommends._TechRecommend").alias("recommendation"),
                    col("RepairOrder.ServVehicle.VehicleServInfo._LastRODate").alias("repair_order_close_date"),
                    col("RepairOrder.CustRecord.ContactInfo.Email").alias("email"),
                    col("RepairOrder.CustRecord.ContactInfo._FirstName").alias("firstname"),
                    col("RepairOrder.CustRecord.ContactInfo._LastName").alias("lastname")
                ).withColumn("DealerNumber", lit(dealer_number))

        # Convert the flattened DataFrame back to a DynamicFrame
        current_dyf = DynamicFrame.fromDF(
            current_df, self.glueContext, "current_dyf"
        ) 
        return current_dyf


    def read_data_from_catalog(self, database, catalog_table, transformation_ctx):
        """Read data from the AWS catalog and return a dynamic frame."""
        return self.glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=catalog_table,
            transformation_ctx=transformation_ctx
        )

    def upsert_consumer(self, cursor, row, phone, email, postal_code, opt_in=None):
        """Upsert consumer data."""

        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealers = cursor.fetchone()
        if dealers is not None:
            dealer_id = dealers[0]
            if opt_in:
                cursor.execute("""
                    INSERT INTO consumer (
                        first_name, last_name, home_phone, postal_code, 
                        email, dealer_id, dealer_customer_no, 
                        email_optin_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (row['firstname'], row['lastname'], phone, postal_code, email, dealer_id, row['dealer_customer_no'], opt_in))
            else:
                cursor.execute("""
                    INSERT INTO consumer (first_name, last_name, home_phone, postal_code, email, dealer_id, dealer_customer_no)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (row['firstname'], row['lastname'], phone, postal_code, email, dealer_id, row['dealer_customer_no']))                  

    def upsert_dealer(self, cursor, row):
        """Upsert dealer data."""

        cursor.execute("SELECT COUNT(*) FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        result = cursor.fetchone()
        if result[0] == 0:
            cursor.execute("""
                INSERT INTO dealer (dms_id)
                VALUES (%s)
            """, (row['DealerNumber'],)) 

    def upsert_op_code(self, cursor, row, op_code):
        """Upsert op code and op_code_repair_order data."""

        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealer = cursor.fetchone()

        if dealer is not None:
            dealer_id = dealer[0]
            cursor.execute("SELECT id FROM service_repair_order WHERE repair_order_no=%s AND dealer_id=%s", (str(row['repair_order_no']), dealer_id))
            service_repair = cursor.fetchone()

            if service_repair is not None:
                service_repair_id = service_repair[0]
                for code, desc in op_code.items():
                    cursor.execute("""
                        INSERT INTO op_code (dealer_id, op_code, op_code_desc)
                        VALUES (%s, %s, %s)
                        ON CONFLICT ON CONSTRAINT unique_op_code DO UPDATE
                        SET op_code_desc = excluded.op_code_desc
                        RETURNING id
                    """, (dealer_id, code, desc))
                    
                    op_code_row = cursor.fetchone()

                    if op_code_row:
                        cursor.execute("""
                            INSERT INTO op_code_repair_order (op_code_id, repair_order_id)
                            VALUES (%s, %s)
                        """, (op_code_row[0], service_repair_id))


    def upsert_vehicle_sale(self, cursor, row, warranty_info, date_of_state_inspection, is_new, phone, email, postal_code):
        """Upsert Vehicle Sale data."""

        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealer = cursor.fetchone()
        if dealer is not None:
            dealer_id = dealer[0]           
            cursor.execute("SELECT id FROM consumer WHERE dealer_id=%s AND email=%s", (
                dealer_id, email
                ))
            consumer = cursor.fetchone()
            if consumer is not None:
                consumer_id = consumer[0] 
                year = None
                if row['year'] is not None:
                    year = int(row['year'])
                cursor.execute("""
                    INSERT INTO vehicle_sale (
                        cost_of_vehicle, discount_on_price,
                        date_of_state_inspection, days_in_stock,
                        mileage_on_vehicle, trade_in_value,
                        payoff_on_trade, profit_on_sale,
                        vehicle_gross, extended_warranty, is_new,
                        value_at_end_of_lease, miles_per_year, has_service_contract,
                        oem_msrp, year, vehicle_class, model, deal_type,
                        warranty_expiration_date,
                        sale_date, vin, dealer_id, consumer_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_vehicle_sale
                    DO NOTHING;
                """,
                (row['cost_of_vehicle'], row['discount_on_price'],
                date_of_state_inspection, row['days_in_stock'], row['mileage_on_vehicle'],
                row['trade_in_value'], row['payoff_on_trade'], row['profit_on_sale'], 
                row['vehicle_gross'], warranty_info['warranty_info_json'], is_new,
                row['value_at_end_of_lease'], row['miles_per_year'], warranty_info['service_cont'],
                row['oem_msrp'],year,row['vehicle_class'],row['model'], row['deal_type'],
                warranty_info['warranty_expiration_date'], row['sale_date'], row['vin'], dealer_id, consumer_id))


    def upsert_service_repair_order(self, cursor, row, email, phone, postal_code, repair_order_open_date, repair_order_close_date):
        """Upsert Service Repair Order to RDS."""
        cursor.execute("SELECT id FROM dealer WHERE dms_id=%s", (row['DealerNumber'],))
        dealer = cursor.fetchone()

        if dealer is not None:
            dealer_id = dealer[0]
            cursor.execute("""
                SELECT id FROM consumer WHERE dealer_id=%s AND email=%s
            """, (dealer_id, email))
            consumer = cursor.fetchone()
            if consumer is not None:
                consumer_id = consumer[0]
                cursor.execute("""
                    INSERT INTO service_repair_order (
                        repair_order_no, ro_open_date,
                        recommendation, ro_close_date,
                        consumer_total_amount,
                        dealer_id, consumer_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unique_ros_dms DO UPDATE
                    SET ro_open_date = excluded.ro_open_date,
                        recommendation = excluded.recommendation,
                        ro_close_date = excluded.ro_close_date,
                        consumer_total_amount = excluded.consumer_total_amount,
                        consumer_id = excluded.consumer_id
                """, (row['repair_order_no'], repair_order_open_date,
                    row['recommendation'], repair_order_close_date,
                    row['consumer_total_amount'],
                    dealer_id, consumer_id))


    def format_phone_number(self, row):
        """Get phone_number struct or array and return string."""

        phone = None
        if hasattr(row, 'phone'):
            if hasattr(row.phone, 'array') and row.phone.array is not None:
                phone = next((phone_row._Num.long for phone_row in row.phone.array if phone_row._Type == 'H'), None)
            elif hasattr(row.phone, 'struct') and row.phone.struct is not None:
                if row.phone.struct._Type == 'H':
                    phone = row.phone.struct._Num

        return str(phone) if phone is not None else phone


    def format_email(self, row):
        """Get email tuple and return string."""

        email = None
        if hasattr(row, 'email') and row.email is not None:
            email = row.email._MailTo 
        return str(email) if email is not None else email

    def format_postal_code(self, row):
        """Get address struct or array and return postal_code."""
        postal_code = None
        if hasattr(row, 'address') and row.address is not None:
            if hasattr(row.address, 'struct') and row.address.struct is not None:
                postal_code = row.address.struct._Zip
            elif hasattr(row.address, 'array') and row.address.array is not None:
                postal_code = next((address_row._Zip for address_row in row.address.array), None)

        return str(postal_code) if postal_code is not None else postal_code

    def format_warranty_info(self, row):
        """Get warranty data struct or array and return JSON."""
        has_array = False
        has_struct = False
        warranty_info = {
            'warranty_info_json' : None,
            'service_cont': False,
            'warranty_expiration_date': None
        }
        if hasattr(row, 'warranty_info') and row.warranty_info is not None:

            if hasattr(row.warranty_info, 'array') and row.warranty_info.array is not None:
                warranty_info['service_cont']  = bool(next((item for item in row.warranty_info.array if item.ServiceCont and item.ServiceCont._ServContYN == 'Y'), None))
                warranty_info['warranty_info_json'] = dumps([item.asDict(True) for item in row.warranty_info.array]) 
                expiration_dates = [item.ExtWarranty.VehExtWarranty._ExpirationDate for item in row.warranty_info.array if item.ExtWarranty and item.ExtWarranty.VehExtWarranty]
                warranty_expiration_date = next((ed for ed in expiration_dates if ed is not None), None)
                warranty_info['warranty_expiration_date'] = self.convert_date(warranty_expiration_date) if warranty_expiration_date is not None else None

            elif hasattr(row.warranty_info, 'struct') and row.warranty_info.struct is not None:
                warranty_info['service_cont']  = bool(row.warranty_info.struct.ServiceCont._ServContYN == 'Y')
                warranty_info['warranty_info_json'] = dumps(row.warranty_info.struct.asDict(True)) 
                warranty_expiration_date = row.warranty_info.struct.ExtWarranty.VehExtWarranty._ExpirationDate
                warranty_info['warranty_expiration_date'] = self.convert_date(warranty_expiration_date) if warranty_expiration_date is not None else None

        return warranty_info

    def format_is_new(self, row):
        """Get NewUsed, determine and return is_new Boolean."""
        is_new = False
        if hasattr(row, 'NewUsed'):
            is_new = True if row.NewUsed == 'N' else False
        return is_new

    def convert_date(self, date_string, input_format='%m/%d/%Y', output_format='%Y-%m-%d'):
        """Convert date object to string."""
        dt_obj = datetime.datetime.strptime(date_string, input_format)
        return dt_obj.strftime(output_format)

    def format_date(self, row, attribute_name):
        """Format and return string datetime value."""
        ro_date = None
        if hasattr(row, attribute_name):
            date_value = getattr(row, attribute_name)
            if date_value is not None:
                ro_date = self.convert_date(date_value)

        return ro_date
    def format_opt_in(self, row):
        opt_in = False
        if hasattr(row, 'OptOut'):
            opt_in = True if row.OptOut == 'N' else False

        return opt_in

    def format_op(self, row):
        """Format and return dictonary of OP Codes and Descriptions."""

        op_codes_dict = {}
        if hasattr(row, 'opcodes') and row.opcodes is not None:
            opcodes = row.opcodes
            for opcode in opcodes:
                op_codes_dict[opcode._RecSvcOpCode] = opcode._RecSvcOpCdDesc

        return op_codes_dict                

    def upsert(self, df, table_name, catalog_table):
        """Upsert ReyRey data to RDS."""

        def format_data(row, *args):
            data = {}
            for arg in args:
                if arg == 'phone':
                    data[arg] = self.format_phone_number(row)
                elif arg == 'email':
                    data[arg] = self.format_email(row)
                elif arg == 'postal_code':
                    data[arg] = self.format_postal_code(row)
                elif arg == 'warranty_info':
                    data[arg] = self.format_warranty_info(row)
                elif arg == 'date_of_state_inspection':
                    data[arg] = self.format_date(row, 'date_of_state_inspection')
                elif arg == 'is_new':
                    data[arg] = self.format_is_new(row)
                elif arg == 'repair_order_open_date':
                    data[arg] = self.format_date(row, 'repair_order_open_date')
                elif arg == 'repair_order_close_date':
                    data[arg] = self.format_date(row, 'repair_order_close_date')
                elif arg == 'op_code':
                    data[arg] = self.format_op(row)
                elif arg == 'opt_in':
                    data[arg] = self.format_opt_in(row)
            return data

        upsert_functions = {
            'consumer': (self.upsert_consumer, ['phone', 'email', 'postal_code']),
            'vehicle_sale': (self.upsert_vehicle_sale, [
                'warranty_info', 
                'date_of_state_inspection',
                'is_new', 'phone', 'email', 
                'postal_code']),
            'op_code': (self.upsert_op_code, ['op_code']),
            'dealer': (self.upsert_dealer, []),
            'service_repair_order': (self.upsert_service_repair_order, [
                'email', 'phone', 'postal_code',
                'repair_order_open_date', 'repair_order_close_date'])
        }

        upsert_function, format_args = upsert_functions[table_name]

        try:
            total_upload = 0
            with self.pool.getconn() as conn:
                with conn.cursor() as cursor:
                    for row in df.rdd.toLocalIterator():
                        try:
                            # Repair Order does not have Customer Opt In
                            if table_name == 'consumer' and 'fi_closed_deal' in catalog_table:
                                format_args.append('opt_in')

                            formatted_data = format_data(row, *format_args)
                            upsert_function(cursor, row, **formatted_data)

                            conn.commit()
                            total_upload += cursor.rowcount

                        except Exception as e:
                            logging.error(f"Error processing row: {row}. Error: {e}")
                            conn.rollback()
                            raise e
                if total_upload <= 0:
                    raise RuntimeError(f"Error ETL Job upserted {total_upload} rows")
        except Exception as e:
            logging.error(f"Error getting connection from pool: {e}")
            raise e
        finally:
            self.pool.putconn(conn)


    def run(self, database):
        """Run ETL for each table in our catalog."""

        for catalog_table in self.catalog_table_names:
            transformation_ctx = 'datasource0_' + catalog_table
            datasource0 = self.read_data_from_catalog(database=database, catalog_table=catalog_table, transformation_ctx=transformation_ctx)
            if datasource0.count() != 0:
                for table_name in self.upsert_table_order[catalog_table]:
                    # Apply mappings to the DynamicFrame
                    df_transformed = self.apply_mappings(datasource0, table_name, catalog_table)
                    # Convert the DynamicFrame to a DataFrame
                    df = df_transformed.toDF()
                    # Perform upsert based on the table_name
                    self.upsert(df, table_name, catalog_table)
            else:
                logging.error(f"There is no new data for the job to process.")

        self.job.commit()




if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "db_name", "catalog_table_names", "catalog_connection", "environment"])
    SM_CLIENT = boto3.client('secretsmanager')
    isprod = args["environment"] == 'prod'

    # Create database connection pool
    DB_CREDENTIALS = _load_secret()

    pool = psycopg2.pool.SimpleConnectionPool(
        1, #Minimum number of connections 
        5, #Maximum number of connections 
        dbname=DB_CREDENTIALS['db_name'],
        host=DB_CREDENTIALS['host'],
        port=DB_CREDENTIALS['port'],
        user=DB_CREDENTIALS['user'],
        password=DB_CREDENTIALS['password']
    )

    job = ReyReyUpsertJob(args, pool=pool)    
    job.run(database=args["db_name"])
    