"""Rey Rey ETL Job."""

import sys
from awsglue.transforms import RenameField, Relationalize, ApplyMapping
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from json import loads

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "db_name", "catalog_table_names", "catalog_connection", "environment"]
)

SM_CLIENT = boto3.client('secretsmanager')
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)
isprod = args["environment"] == 'prod'


class ReyReyUpsertJob:
    """Create object to perform ETL."""


    def __init__(self, args):
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(args['JOB_NAME'], args)
        self.DB_CONFIG = _load_secret()
        self.catalog_table_names = args['catalog_table_names']
        self.upsert_table_order = get_upsert_table_order()


    def get_upsert_table_order():
        """Return a list of tables to upsert by order of dependency."""

        upsert_table_order = {}
        for name in self.catalog_table_names:
            if 'fi_closed_deal' in name:
                upsert_table_order[name] = ['consumer', 'vehicle', 'vehicle_sale']
            elif 'repair_order' in name:
                upsert_table_order[name] = ['dealer', 'vehicle', 'consumer', 'service_repair_order']

        return upsert_table_order


    def _load_secret():
        """Get DMS DB configuration from Secrets Manager."""

        secret_string = loads(SM_CLIENT.get_secret_value(
            SecretId='prod/DMSDB' if isprod else 'stage/DMSDB' 
        )['SecretString'])
        DB_CONFIG = {
            'target_database_name': secret_string["db_name"],
            'target_db_jdbc_url': secret_string['jdbc_url'],
            'target_db_user': secret_string['user'],
            'target_db_password': secret_string['password']
        }
        return DB_CONFIG

    def apply_mapping(self, tablename, catalog_table):
        """Apply appropriate mapping to dynamic frame."""
            #for all of the comments in each mapping this customization can be added in the partition methods.
            mapping = []

            if 'fi_closed_deal' in catalog_table:
                if tablename == 'consumer':
                    mappings = [
                        ("rey_ImpelFIClosedDeal.FIDeal.Buyer.CustRecord.ContactInfo.FirstName", "string", "firstname", "string"),
                        ("rey_ImpelFIClosedDeal.FIDeal.Buyer.CustRecord.ContactInfo.LastName", "string", "lastname", "string"),
                        ("rey_ImpelFIClosedDeal.FIDeal.Buyer.CustRecord.ContactInfo.Email.MailTo", "string", "email", "string"),
                        ("rey_ImpelFIClosedDeal.FIDeal.Buyer.CustRecord.ContactInfo.Address.Zip", "string", "postal_code", "string"),
                        ("rey_ImpelFIClosedDeal.FIDeal.Buyer.CustRecord.ContactInfo.Phone", "string", "phone_numbers", "arrays")
                    ]
                    #additional work for grabbing the phone number might be required
                    #check if consumer exists before upsert
                elif tablename == 'vehicle':
                    mappings = [
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.Vehicle.Vin", "string", "vin", "string"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleYr", "string", "year", "int"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail.VehClass", "string", "vehicle_class", "string"),
                        ("rey_ImpelFIClosedDeal.ApplicationArea.Sender.DealerNumber", "string", "dealernumber", "string")
                    ]
                    #for vehicle before inserting we need to find the appropriate dealer id. if it doesn't exist we need to create it and grab it.
                    #before upsert dealer id needs to be grabbed
                elif tablename == 'vehicle_sale':
                    mappings = [
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.VehCost", "string", "cost_of_vehicle", "double"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.Discount", "string", "discount_on_price", "double"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.InspectionDate", "string", "date_of_state_inspection", "timestamp")
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.DaysInStock", "string", "days_in_stock", "int"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail.OdomReading", "string", "mileage_on_vehicle", "int"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TradeIn.ActualCashValue", "string", "trade_in_value", "double"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TradeIn.Payoff", "string", "payoff_on_trade", "double"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.Recap.Reserves.NetProfit", "string", "profit_on_sale", "double"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.Recap.Reserves.VehicleGross", "string", "vehicle_gross", "double"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.WarrantyInfo", "string", "warranty_info", "array"),
                        ("rey_ImpelFIClosedDeal.FIDeal.FIDealFin.TransactionVehicle.Vehicle.VehicleDetail.NewUsed", "string", "NewUsed", "string"),
                        ("rey_ImpelFIClosedDeal.ApplicationArea.Sender.DealerNumber", "string", "dealernumber", "string")
                    ]                   

                    #bool values newused need to be extracted to appropriate types
                    #dump warranty info as a json into the table in the catch all field
                    #vehicle id(use vin) and dealer id(use dealernumber) needs to be grabbed before upsert


            elif 'repair_order' in catalog_table:
                if tablename == 'dealer':
                     mappings = [
                        ("rey_ImpelRepairOrder.ApplicationArea.DealerNumber", "string", "dealernumber", "string"),
                    ]                
                    #before upserting needs to be checked for existence
                elif tablename == 'vehicle':
                    mappings = [
                        ("rey_ImpelRepairOrder.RepairOrder.RoRecord.Rogen.Vin", "string", "vin", "string"),
                        ("rey_ImpelRepairOrder.ApplicationArea.DealerNumber", "string", "dealernumber", "string"),
                    ]               
                    #before upsert dealer id needs to be grabbed.
                elif tablename == 'consumer':
                    mappings = [
                        ("rey_ImpelRepairOrder.RepairOrder.CustRecord.ContactInfo.FirstName", "string", "firstname", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.CustRecord.ContactInfo.LastName", "string", "lastname", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.CustRecord.ContactInfo.Phone", "string", "phone", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.CustRecord.ContactInfo.Email", "string", "email", "string")
                    ]
                    #check if consumer exists before upsert. check by email 
                elif tablename == 'service_repair_order':
                    mappings = [
                        ("rey_ImpelRepairOrder.RepairOrder.RoRecord.Rogen.Vin", "string", "vin", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.RoRecord.Rogen.RoNo", "string", "repair_order_no", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.RoRecord.Rogen.RoCreateDate", "string", "repair_order_open_date", "timestamp"),
                        ("rey_ImpelRepairOrder.RepairOrder.RoRecord.Rogen.CustRoTotalAmt", "string", "consumer_total_amount", "double"),
                        ("rey_ImpelRepairOrder.RepairOrder.RoRecord.Rogen.RecommendedServc", "string", "recommendation", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.ServVehicle.VehicleServInfo.LastRODate", "string", "repair_order_close_date", "timestamp"),
                        ("rey_ImpelRepairOrder.ApplicationArea.DealerNumber", "string", "dealernumber", "string"),
                        ("rey_ImpelRepairOrder.RepairOrder.CustRecord.ContactInfo.Email", "string", "email", "string")
                    ]  
                    #before upsert vehicle id needs to be grabbed by the vin and included. dealer id(use dealer number) also needs to be included. consumer id(use email) also needs to be included.
    def read_data_from_catalog(self, database, catalog_table):
        """Read data from the AWS catalog and return a dynamic frame."""

        return self.glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=catalog_table,
            transformation_ctx="datasource0"
        )


    def run(self, database, table_names):
        """Run ETL for each table in our catalog."""

        for catalog_table in self.catalog_table_names:

            for table_name in self.upsert_table_order[catalog_table]:
                datasource0 = self.read_data_from_catalog(database=database, catalog_table=catalog_table)

                # Apply mappings to the DynamicFrame
                df_transformed = self.apply_mappings(datasource0, table_name, catalog_table)

                # Convert DynamicFrame to DataFrame
                df = df_transformed.toDF()

                # # Perform upsert based on the table_name
                # if table_name == 'consumer':
                #     df.rdd.foreachPartition(self.upsert_consumer_partition)
                # elif table_name == 'vehicle':
                #     df.rdd.foreachPartition(self.upsert_vehicle_partition)
                # elif table_name == 'vehicle_sale':
                #     df.rdd.foreachPartition(self.upsert_vehicle_sale_partition)
                # elif table_name == 'dealer':
                #     df.rdd.foreachPartition(self.upsert_dealer_partition)
                # elif table_name == 'service_repair_order':
                #     df.rdd.foreachPartition(self.upsert_deal_partition)
                # else:
                #     raise ValueError(f"Invalid table name: {table_name}")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = ReyReyUpsertJob(args)    
    job.run(database=args["db_name"])