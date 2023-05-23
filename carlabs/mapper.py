from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Generator
from pandas import DataFrame
from datetime import datetime, date
import re


class DMSDataSource(str, Enum):
    CDK = 'CDK'
    DEALERVAULT = 'DEALERVAULT'
    DEALERTRACK = 'DEALERTRACK'


@dataclass
class VehicleSale:
    data_source: DMSDataSource
    sale_date: Optional[date] = ''
    listed_price: Optional[str] = ''
    sales_tax: Optional[str] = ''
    mileage_on_vehicle: Optional[str] = ''
    deal_type: Optional[str] = ''
    cost_of_vehicle: Optional[str] = ''
    oem_msrp: Optional[str] = ''
    adjustment_on_price: Optional[str] = ''
    days_in_stock: Optional[str] = ''
    date_of_state_inspection: Optional[str] = ''
    new_or_used: Optional[str] = ''
    trade_in_value: Optional[str] = ''
    payoff_on_trade: Optional[str] = ''
    value_at_end_of_lease: Optional[str] = ''
    miles_per_year: Optional[str] = ''
    profit_on_sale: Optional[str] = ''
    has_service_contract: Optional[str] = ''
    vehicle_gross: Optional[str] = ''
    warranty_expiration_date: Optional[str] = ''
    service_package_flag: Optional[str] = ''
    db_creation_date: Optional[str] = ''
    vin: Optional[str] = ''
    make: Optional[str] = ''
    model: Optional[str] = ''
    year: Optional[str] = ''
    delivery_date: Optional[str] = ''
    finance_rate: Optional[str] = ''
    finance_term: Optional[str] = ''
    finance_amount: Optional[str] = ''
    date_of_inventory: Optional[date] = ''


class VehicleSaleMapper(ABC):

    @abstractmethod
    def transform(self, dms_data: DataFrame) -> Generator[VehicleSale, None, None]:
        ...


def date_from_str(raw_date: str):
    if not re.fullmatch(r'[0-9]{8}', raw_date):
        return None
    return datetime.strptime(raw_date, '%Y%m%d').date()

class DealertrackMapper(VehicleSaleMapper):

    def date_from_str(self, raw_date: str):
        if not re.fullmatch(r'[0-9]{8}', raw_date):
            return None
        return datetime.strptime(raw_date, '%Y%m%d').date()

    def transform(self, dms_data: DataFrame) -> Generator[VehicleSale, None, None]:
        imported_data = dms_data['importedData']

        sale_date = self.date_from_str(imported_data['DealDate'])
        date_of_inventory = self.date_from_str(imported_data['vehicle']['DateInInventory'])
        delivery_date = self.date_from_str(imported_data['vehicle']['DateDelivered'])
        days_in_stock = None
        if sale_date and date_of_inventory:
            days_in_stock = (sale_date - date_of_inventory).days

        yield VehicleSale(
            sale_date=sale_date,
            listed_price=imported_data['deal']['detail']['RetailPrice'],
            sales_tax=imported_data['deal']['detail']['Tax1'],
            mileage_on_vehicle=imported_data['vehicle']['Odometer'],
            deal_type=imported_data['deal']['detail']['SaleType'],
            cost_of_vehicle=imported_data['vehicle']['VehicleCost'],
            oem_msrp=imported_data['deal']['detail']['MSRP'],
            adjustment_on_price=imported_data['deal']['detail']['AdjCapitalizedCost'],
            days_in_stock=days_in_stock,
            new_or_used=imported_data['VehicleType'],
            payoff_on_trade=imported_data['deal']['detail']['TradePayoff'],
            miles_per_year=imported_data['deal']['detail']['MilesYearActual'],
            has_service_contract=len(imported_data['deal']['detail']['ServiceContracts']) != 0,
            vehicle_gross=imported_data['deal']['detail']['RetailPrice'],
            warranty_expiration_date=imported_data['vehicle']['WarrantyMonths'],
            service_package_flag=len(imported_data['deal']['detail']['ServiceContracts']) != 0,
            db_creation_date=dms_data['creationDate'],
            vin=imported_data['deal']['VIN'],
            make=imported_data['deal']['Make'],
            model=imported_data['deal']['Model'],
            year=imported_data['deal']['ModelYear'],
            delivery_date=delivery_date,
            finance_rate=imported_data['deal']['detail']['APR'],
            finance_term=imported_data['deal']['detail']['RetailTerm'],
            finance_amount=imported_data['deal']['detail']['AmountFinanced'],
            date_of_inventory=date_of_inventory,
            data_source=DMSDataSource.DEALERTRACK
        )


class CDKMapper(VehicleSaleMapper):

    def date_from_str(self, raw_date: str):
        if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', raw_date):
            return None
        return datetime.strptime(raw_date, '%Y-%m-%d').date()

    def transform(self, dms_data: DataFrame) -> Generator[VehicleSale, None, None]:
        for imported_data in dms_data['importedData']:
            sale_date = self.date_from_str(imported_data['DealEvent1Date'][0])
            delivery_date = self.date_from_str(imported_data['PickupDate1'][0])
            yield VehicleSale(
                sale_date=sale_date,
                listed_price=imported_data['OutTheDoorPrice'][0],
                sales_tax=imported_data['Tax1Amount'][0],
                mileage_on_vehicle=imported_data['VehicleMileage'][0],
                deal_type=imported_data['SaleType'][0],
                cost_of_vehicle=imported_data['CostPrice'][0],
                oem_msrp=imported_data['MSRP'][0],
                adjustment_on_price=imported_data['AdjustedCostofVehicle'][0],
                new_or_used=imported_data['FIDealType'][0],
                trade_in_value=imported_data['NetTrade1'][0],
                value_at_end_of_lease=imported_data['LeaseEndValue'][0],
                miles_per_year=imported_data['LeaseMileageAllowance'][0],
                profit_on_sale=imported_data['FrontEndGrossProfit'][0],
                has_service_contract=imported_data['Insurance1Name'][0] != '',
                vehicle_gross=imported_data['SalePriceWithWeOwes'][0],
                service_package_flag=imported_data['Insurance1Name'][0] != '',
                db_creation_date=dms_data['creationDate'],
                vin=imported_data['VIN'][0],
                make=imported_data['MakeName'][0],
                model=imported_data['ModelName'][0],
                year=imported_data['Year'][0],
                delivery_date=delivery_date,
                finance_rate=imported_data['APR'][0],
                finance_term=imported_data['Term'][0],
                finance_amount=imported_data['FeeOption10Amount'][0],
                data_source=DMSDataSource.CDK
            )


class DealervaultMapper(VehicleSaleMapper):

    def date_from_str(self, raw_date: str):
        if not isinstance(raw_date, str) or not re.fullmatch(r'[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}', raw_date):
            return None
        return datetime.strptime(raw_date, '%m/%d/%Y').date()

    def transform(self, dms_data: DataFrame) -> Generator[VehicleSale, None, None]:
        for imported_data in dms_data['importedData']:
            sale_date = self.date_from_str(imported_data['Contract Date'])
            date_of_inventory = self.date_from_str(imported_data['Inventory Date'])
            delivery_date = self.date_from_str(imported_data['Delivery Date'])
            days_in_stock = None
            if sale_date and date_of_inventory:
                days_in_stock = (sale_date - date_of_inventory).days
            yield VehicleSale(
                sale_date=sale_date,
                listed_price=imported_data['List Price'],
                sales_tax=imported_data['Total Tax'],
                mileage_on_vehicle=imported_data['Mileage'],
                deal_type=imported_data['Deal Type'],
                cost_of_vehicle=imported_data['Cost'],
                oem_msrp=imported_data['MSRP'],
                adjustment_on_price=imported_data['Adjusted Cost'],
                days_in_stock=days_in_stock,
                new_or_used=imported_data['New/Used'],
                trade_in_value=imported_data['Trade 1 Actual Cash Value'],
                value_at_end_of_lease=imported_data['Lease Depreciation Value'],
                miles_per_year=imported_data['Allowed Miles'],
                profit_on_sale=imported_data['Total Profit'],
                has_service_contract=imported_data['Insurance Profit'] != '',
                vehicle_gross=imported_data['Sales Price'],
                warranty_expiration_date=imported_data['Warranty 1 Term'],
                service_package_flag=imported_data['Warranty 1 Name'] != '',
                db_creation_date=dms_data['creationDate'],
                vin=imported_data['VIN'],
                make=imported_data['Make'],
                model=imported_data['Model'],
                year=imported_data['Year'],
                delivery_date=delivery_date,
                finance_rate=imported_data['APR'],
                finance_term=imported_data['Term'],
                finance_amount=imported_data['Amount Financed'],
                date_of_inventory=date_of_inventory,
                data_source=DMSDataSource.DEALERVAULT
            )


def get_dms_mapper_class(source: DMSDataSource) -> VehicleSaleMapper:
    dms_to_mapper = {
        DMSDataSource.CDK: CDKMapper,
        DMSDataSource.DEALERTRACK: DealertrackMapper,
        DMSDataSource.DEALERVAULT: DealervaultMapper
    }

    return dms_to_mapper[source]