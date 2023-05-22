from pytest import fixture
from pandas import DataFrame
from .data.cdk import CDK_DATA
from .data.dealertrack import DEALERTRACK_DATA
from .data.dealervault import DEALERVAULT_DATA
from app.transformers.sales_history.cdk import CDKTransformer
from app.transformers.sales_history.dealertrack import DealertrackTransformer
from app.transformers.sales_history.dealervault import DealervaultTransformer
import copy

@fixture
def cdk_data():
    df = DataFrame.from_records(CDK_DATA)
    return df.iloc[0].to_dict()

@fixture
def dealertrack_data():
    return DEALERTRACK_DATA

@fixture
def dealervault_data():
    return DEALERVAULT_DATA

@fixture
def cdk_transformer(cdk_data):
    return CDKTransformer(carlabs_data=cdk_data)

@fixture
def dealertrack_transformer(dealertrack_data):
    return DealertrackTransformer(carlabs_data=dealertrack_data)

@fixture
def dealervault_transformer(dealervault_data):
    return DealervaultTransformer(carlabs_data=dealervault_data)

# @fixture
# def expected_cdk_vehicle_sale():
#     return VehicleSale(
#         sale_date="2023-01-04",
#         listed_price="27709.24",
#         sales_tax="1329.24",
#         mileage_on_vehicle="10003",
#         deal_type="Cash",
#         cost_of_vehicle="22327.50",
#         oem_msrp="",
#         adjustment_on_price="0.00",
#         days_in_stock = Column(Integer)
#         date_of_state_inspection = Column(DateTime)
#         is_new="Used",
#         trade_in_value="2000.00",
#         payoff_on_trade = Column(Float)
#         value_at_end_of_lease="",
#         miles_per_year="",
#         profit_on_sale="2072.50",
#         has_service_contract = Column(Boolean)
#         vehicle_gross="23900.00",
#         warranty_expiration_date = Column(DateTime)
#         service_package_flag = Column(Boolean)
#         db_creation_date = Column(DateTime)
#         vin="2GNAXJEV2L6188291",
#         make="CHEVROLET",
#         model="EQUINOX",
#         year="2020"
#         delivery_date = Column(DateTime)
#         finance_rate = Column(Float)
#         finance_term = Column(Float)
#         finance_amount = Column(Float)
#         date_of_inventory = Column(DateTime)
#         si_load_process = Column(String)
#         si_load_timestamp = Column(DateTime)
#     )