from orm.models.shared_dms import VehicleSale, Consumer
from .base import BaseTransformer
from mappings import VehicleTableMapping, VehicleSaleTableMapping, ConsumerTableMapping, ServiceContractTableMapping


class CDKTransformer(BaseTransformer):

    vehicle_sale_table_mapping = VehicleSaleTableMapping(
        sale_date='importedData.DealEvent1Date',
        listed_price='importedData.OutTheDoorPrice',
        sales_tax='importedData.Tax1Amount',
        mileage_on_vehicle='importedData.VehicleMileage',
        deal_type='importedData.SaleType',
        cost_of_vehicle='importedData.CostPrice',
        oem_msrp='importedData.MSRP',
        adjustment_on_price='importedData.AdjustedCostofVehicle',
        trade_in_value='importedData.NetTrade1',
        value_at_end_of_lease='importedData.LeaseEndValue',
        miles_per_year='importedData.LeaseMileageAllowance',
        profit_on_sale='importedData.FrontEndGrossProfit',
        vehicle_gross='importedData.SalePriceWithWeOwes',
        delivery_date='importedData.PickupDate1',
        finance_rate='importedData.APR',
        finance_term='importedData.Term',
        finance_amount='importedData.FeeOption10Amount',
        date_of_inventory=None,
        date_of_state_inspection=None,
        payoff_on_trade=None,
        warranty_expiration_date=None,
        has_service_contract='importedData.Insurance1Name',
        service_package_flag='importedData.Insurance1Name'
    )

    consumer_table_mapping = ConsumerTableMapping(
        dealer_customer_no=None,
        first_name='importedData.user.firstName',
        last_name='importedData.user.lastName',
        email='importedData.Email1',
        ip_address=None,
        cell_phone='importedData.user.sms',
        city=None,
        state='importedData.State',
        metro=None,
        postal_code='importedData.ZipOrPostalCode',
        home_phone='importedData.HomePhone',
        email_optin_flag=None,
        phone_optin_flag=None,
        postal_mail_optin_flag=None,
        sms_optin_flag=None,
        master_consumer_id=None,
        address='importedData.Address'
    )

    vehicle_table_mapping = VehicleTableMapping(
        vin='importedData.VIN',
        oem_name=None,
        type=None,
        vehicle_class=None,
        mileage='importedData.VehicleMileage',
        make='importedData.MakeName',
        model='importedData.ModelName',
        year='importedData.Year',
        new_or_used='importedData.FIDealType',
    )

    service_contract_table_mapping = ServiceContractTableMapping(
        contract_name='importedData.Insurance1Name',
        start_date='importedData.ContractDate',
        amount='importedData.Ins1Income',
        cost='importedData.Ins1Cost',
        deductible='importedData.Insurance1Deductible',
        expiration_months='importedData.Term',
        expiration_miles='importedData.Insurance1LimitMiles'
    )

    date_format = '%Y-%m-%d'

    def pre_process_data(self):
        for _, field in self.vehicle_sale_table_mapping.fields:
            if isinstance(self.carlabs_data[field], list):
                self.carlabs_data[field] = self.carlabs_data[field][0]

        for _, field in self.consumer_table_mapping.fields:
            if isinstance(self.carlabs_data[field], list):
                self.carlabs_data[field] = self.carlabs_data[field][0]

        for _, field in self.vehicle_table_mapping.fields:
            if isinstance(self.carlabs_data[field], list):
                self.carlabs_data[field] = self.carlabs_data[field][0]

        for _, field in self.service_contract_table_mapping.fields:
            if isinstance(self.carlabs_data[field], list):
                self.carlabs_data[field] = self.carlabs_data[field][0]

    def post_process_consumer(self, orm: Consumer) -> Consumer:
        never_contact = self.carlabs_data['importedData.user.doNotContact.ever']
        sms_contact = False if never_contact else not self.carlabs_data['importedData.user.doNotContact.sms']
        email_contact = False if never_contact else not self.carlabs_data['importedData.user.doNotContact.email']

        orm.sms_optin_flag = sms_contact
        orm.email_optin_flag = email_contact
        orm.phone_optin_flag = not never_contact
        orm.postal_mail_optin_flag = not never_contact

        return orm

    def post_process_vehicle_sale(self, orm: VehicleSale) -> VehicleSale:
        return orm