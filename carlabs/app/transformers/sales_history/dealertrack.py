from orm.models.shared_dms import VehicleSale, Consumer
from .base import BaseTransformer
from mappings import VehicleTableMapping, VehicleSaleTableMapping, ConsumerTableMapping, ServiceContractTableMapping


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
        trade_in_value=None,
        payoff_on_trade='importedData.deal.detail.TradePayoff',
        value_at_end_of_lease=None,
        miles_per_year='importedData.deal.detail.MilesYearActual',
        profit_on_sale=None,
        vehicle_gross='importedData.deal.detail.RetailPrice',
        warranty_expiration_date='importedData.vehicle.WarrantyMonths',
        delivery_date='importedData.vehicle.DateDelivered',
        finance_rate='importedData.deal.detail.APR',
        finance_term='importedData.deal.detail.RetailTerm',
        finance_amount='importedData.deal.detail.AmountFinanced',
        date_of_inventory='importedData.vehicle.DateInInventory',
        date_of_state_inspection=None,
        has_service_contract='importedData.deal.detail.ServiceContracts.ServiceContract.ContractName',
        service_package_flag='importedData.deal.detail.ServiceContracts.ServiceContract.ContractName'
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

    vehicle_table_mapping = VehicleTableMapping(
        vin='importedData.VIN',
        oem_name=None,
        type='importedData.VehicleType',
        vehicle_class=None,
        mileage='importedData.vehicle.Odometer',
        make='importedData.Make',
        model='importedData.Model',
        year='importedData.ModelYear',
        new_or_used='importedData.VehicleType',
    )

    service_contract_table_mapping = ServiceContractTableMapping(
        contract_name='importedData.deal.detail.ServiceContracts.ServiceContract.ContractName',
        start_date='importedData.deal.detail.ServiceContracts.ServiceContract.ContractStartDate',
        amount='importedData.deal.detail.ServiceContractAmount',
        cost='importedData.deal.detail.ServiceContractCost',
        deductible='importedData.deal.detail.ServiceContracts.ServiceContract.ContractDeductible',
        expiration_months='importedData.deal.detail.ServiceContracts.ServiceContract.ContractExpirationMonths',
        expiration_miles='importedData.deal.detail.ServiceContracts.ServiceContract.ContractExpirationMiles'
    )

    date_format = '%Y%m%d'

    def post_process_consumer(self, orm: Consumer) -> Consumer:
        orm.email_optin_flag = self.carlabs_data.get('importedData.customerInformation.AllowContactByEmail') == 'Y'
        orm.phone_optin_flag = self.carlabs_data.get('importedData.customerInformation.AllowContactByPhone') == 'Y'
        orm.postal_mail_optin_flag = self.carlabs_data.get('importedData.customerInformation.AllowContactByPostal') == 'Y'
        orm.sms_optin_flag = self.carlabs_data.get('importedData.customerInformation.AllowContactByPhone') == 'Y'

        return orm

    def post_process_vehicle_sale(self, orm: VehicleSale) -> VehicleSale:
        orm.has_service_contract = self.carlabs_data.get(self.vehicle_sale_table_mapping.has_service_contract) is not None
        orm.service_package_flag = self.carlabs_data.get(self.vehicle_sale_table_mapping.service_package_flag) is not None
        return orm

    def pre_process_data(self):
        if 'importedData.deal.detail.ServiceContracts.ServiceContract' not in self.carlabs_data:
            return

        service_contract = self.carlabs_data['importedData.deal.detail.ServiceContracts.ServiceContract'][0]
        self.carlabs_data['importedData.deal.detail.ServiceContracts.ServiceContract.ContractName'] = service_contract['ContractName']
        self.carlabs_data['importedData.deal.detail.ServiceContracts.ServiceContract.ContractStartDate'] = service_contract['ContractStartDate']
        self.carlabs_data['importedData.deal.detail.ServiceContracts.ServiceContract.ContractDeductible'] = service_contract['ContractDeductible']
        self.carlabs_data['importedData.deal.detail.ServiceContracts.ServiceContract.ContractExpirationMonths'] = service_contract['ContractExpirationMonths']
        self.carlabs_data['importedData.deal.detail.ServiceContracts.ServiceContract.ContractExpirationMiles'] = service_contract['ContractExpirationMiles']