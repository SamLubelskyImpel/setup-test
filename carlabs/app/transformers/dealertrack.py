from ..orm.models.shared_dms.consumer import Consumer
from .base import BaseTransformer
from ..mappings.vehicle_sale import VehicleSaleTableMapping
from ..mappings.consumer import ConsumerTableMapping


class DealertrackTransformer(BaseTransformer):

    vehicle_sale_table_mapping = VehicleSaleTableMapping(
        sale_date='importedData.DealDate',
        listed_price='importedData.deal.detail.RetailPrice',
        sales_tax='importedData.deal.detail.Tax1',
        mileage_on_vehicle='importedData.vehicle.Odometer',
        deal_type='importedData.deal.detail.SaleType',
        cost_of_vehicle='importedData.vehicle.VehicleCost',
        oem_msrp='importedData.deal.detail.MSRP',
        adjustment_on_price='importedData.deal.detail.AdjCapitalizedCost',
        is_new='importedData.VehicleType',
        trade_in_value=None,
        payoff_on_trade='importedData.deal.detail.TradePayoff',
        value_at_end_of_lease=None,
        miles_per_year='importedData.deal.detail.MilesYearActual',
        profit_on_sale=None,
        vehicle_gross='importedData.deal.detail.RetailPrice',
        warranty_expiration_date='importedData.vehicle.WarrantyMonths',
        vin='importedData.deal.VIN',
        make='importedData.deal.Make',
        model='importedData.deal.Model',
        year='importedData.deal.ModelYear',
        delivery_date='importedData.vehicle.DateDelivered',
        finance_rate='importedData.deal.detail.APR',
        finance_term='importedData.deal.detail.RetailTerm',
        finance_amount='importedData.deal.detail.AmountFinanced',
        date_of_inventory='importedData.vehicle.DateInInventory',
        date_of_state_inspection=None,
        has_service_contract='importedData.deal.detail.ServiceContracts.ServiceContract',
        service_package_flag='importedData.deal.detail.ServiceContracts.ServiceContract'
    )

    consumer_table_mapping = ConsumerTableMapping(
        dealer_customer_no='importedData.BuyerCustomerNumber',
        first_name='importedData.BuyerFirstName',
        last_name='importedData.BuyerLastName',
        email='importedData.customerInformation.Email1',
        ip_address=None,
        cell_phone='importedData.customerInformation.CellPhone',
        city='importedData.customerInformation.City',
        state='importedData.customerInformation.StateCode',
        metro=None,
        postal_code='importedData.customerInformation.ZipCode',
        home_phone='importedData.customerInformation.PhoneNumber',
        email_optin_flag=None,
        phone_optin_flag=None,
        postal_mail_optin_flag=None,
        sms_optin_flag=None,
        master_consumer_id=None,
        address='importedData.customerInformation.Address1'
    )

    date_format = '%Y%m%d'

    def post_process_consumer(self, orm: Consumer) -> Consumer:
        orm.email_optin_flag = self.carlabs_data['importedData.customerInformation.AllowContactByEmail'] == 'Y'
        orm.phone_optin_flag = self.carlabs_data['importedData.customerInformation.AllowContactByPhone'] == 'Y'
        orm.postal_mail_optin_flag = self.carlabs_data['importedData.customerInformation.AllowContactByPostal'] == 'Y'
        orm.sms_optin_flag = self.carlabs_data['importedData.customerInformation.AllowContactByPhone'] == 'Y'

        return orm

    def pre_process_data(self):
        ...