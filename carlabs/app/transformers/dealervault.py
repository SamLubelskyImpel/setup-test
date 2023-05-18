from .base import BaseTransformer
from ..mappings.vehicle_sale import VehicleSaleTableMapping
from ..mappings.consumer import ConsumerTableMapping


class DealervaultTransformer(BaseTransformer):

    vehicle_sale_table_mapping = VehicleSaleTableMapping(
        sale_date='importedData.Contract Date',
        listed_price='importedData.List Price',
        sales_tax='importedData.Total Tax',
        mileage_on_vehicle='importedData.Mileage',
        deal_type='importedData.Deal Type',
        cost_of_vehicle='importedData.Cost',
        oem_msrp='importedData.MSRP',
        adjustment_on_price='importedData.Adjusted Cost',
        is_new='importedData.New/Used',
        trade_in_value='importedData.Trade 1 Actual Cash Value',
        payoff_on_trade=None,
        value_at_end_of_lease='importedData.Lease Depreciation Value',
        miles_per_year='importedData.Allowed Miles',
        profit_on_sale='importedData.Total Profit',
        vehicle_gross='importedData.Sales Price',
        warranty_expiration_date='importedData.Warranty 1 Term',
        vin='importedData.VIN',
        make='importedData.Make',
        model='importedData.Model',
        year='importedData.Year',
        delivery_date='importedData.Delivery Date',
        finance_rate='importedData.APR',
        finance_term='importedData.Term',
        finance_amount='importedData.Amount Financed',
        date_of_inventory='importedData.Inventory Date',
        date_of_state_inspection=None,
        has_service_contract='importedData.Insurance Profit',
        service_package_flag='importedData.Warranty 1 Name',
    )

    consumer_table_mapping = ConsumerTableMapping(
        dealer_customer_no='importedData.Customer Number',
        first_name='importedData.First Name',
        last_name='importedData.Last Name',
        email='importedData.Email 1',
        ip_address=None,
        cell_phone='importedData.Cell Phone',
        city='importedData.City',
        state='importedData.State',
        metro=None,
        postal_code='importedData.Zip',
        home_phone='importedData.Home Phone',
        email_optin_flag='importedData.Block Email',
        phone_optin_flag='importedData.Block Phone',
        postal_mail_optin_flag='importedData.Block Mail',
        sms_optin_flag=None,
        master_consumer_id=None,
        address='importedData.Address Line 1'
    )

    date_format = '%m/%d/%Y'

    def pre_process_data(self):
        ...