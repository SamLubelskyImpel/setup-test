from orm.models.data_imports import DataImports
from orm.models.vehicle_sale import VehicleSale
from utils import parsed_date, parsed_int


def map_sale(record: DataImports, dip_id: int):
    sale = VehicleSale()
    imported_data = record.importedData
    if record.dataSource == 'CDK':
        date_format = '%Y-%m-%d'
        sale.sale_date = parsed_date(
            date_format, imported_data['DealEvent1Date'][0])
        sale.listed_price = imported_data['OutTheDoorPrice'][0]
        sale.sales_tax = imported_data['Tax1Amount'][0]
        sale.mileage_on_vehicle = parsed_int(
            imported_data['VehicleMileage'][0])
        sale.deal_type = imported_data['SaleType'][0]
        sale.cost_of_vehicle = imported_data['CostPrice'][0]
        sale.oem_msrp = imported_data['MSRP'][0]
        sale.adjustment_on_price = imported_data['AdjustedCostofVehicle'][0]
        sale.trade_in_value = imported_data['NetTrade1'][0]
        sale.value_at_end_of_lease = imported_data['LeaseEndValue'][0]
        sale.miles_per_year = int(
            imported_data['LeaseMileageAllowance'][0]) if imported_data['LeaseMileageAllowance'][0] else None
        sale.profit_on_sale = imported_data['FrontEndGrossProfit'][0]
        sale.vehicle_gross = imported_data['SalePriceWithWeOwes'][0]
        sale.delivery_date = parsed_date(
            date_format, imported_data['PickupDate1'][0])
        sale.finance_rate = imported_data['APR'][0]
        sale.finance_term = imported_data['Term'][0]
        sale.finance_amount = imported_data['FeeOption10Amount'][0]
        sale.has_service_contract = imported_data['Insurance1Name'][0] != ''
    elif record.dataSource == 'DealerTrack':
        date_format = '%Y%m%d'
        sale.sale_date = parsed_date(date_format, imported_data['DealDate'])
        sale.listed_price = imported_data['deal']['detail']['RetailPrice']
        sale.sales_tax = imported_data['deal']['detail']['Tax1']
        sale.mileage_on_vehicle = parsed_int(
            imported_data['vehicle']['Odometer'])
        sale.deal_type = imported_data['deal']['detail']['SaleType']
        sale.cost_of_vehicle = imported_data['vehicle']['VehicleCost']
        sale.oem_msrp = imported_data['deal']['detail']['MSRP']
        sale.adjustment_on_price = imported_data['deal']['detail']['AdjCapitalizedCost']
        sale.payoff_on_trade = imported_data['deal']['detail']['TradePayoff']
        sale.miles_per_year = parsed_int(
            imported_data['deal']['detail']['MilesYearActual'])
        sale.vehicle_gross = imported_data['deal']['detail']['RetailPrice']
        sale.delivery_date = parsed_date(
            date_format, imported_data['vehicle']['DateDelivered'])
        sale.finance_rate = imported_data['deal']['detail']['APR']
        sale.finance_term = imported_data['deal']['detail']['RetailTerm']
        sale.finance_amount = imported_data['deal']['detail']['AmountFinanced']
        sale.date_of_inventory = parsed_date(
            date_format, imported_data['vehicle']['DateInInventory'])
        if imported_data['deal']['detail']['ServiceContracts']:
            sale.has_service_contract = len(
                imported_data['deal']['detail']['ServiceContracts']) != 0
    elif record.dataSource == 'DEALERVAULT':
        date_format = '%Y%m%d'
        sale.sale_date = parsed_date(
            date_format, imported_data['Contract Date'])
        sale.listed_price = imported_data['List Price']
        sale.sales_tax = imported_data['Total Tax']
        sale.mileage_on_vehicle = parsed_int(imported_data['Mileage'])
        sale.deal_type = imported_data['Deal Type']
        sale.cost_of_vehicle = imported_data['Cost']
        sale.oem_msrp = imported_data['MSRP']
        sale.adjustment_on_price = imported_data['Adjusted Cost']
        sale.trade_in_value = imported_data['Trade 1 Actual Cash Value']
        sale.value_at_end_of_lease = imported_data['Lease Depreciation Value']
        sale.miles_per_year = parsed_int(imported_data['Allowed Miles'])
        sale.profit_on_sale = imported_data['Total Profit']
        sale.vehicle_gross = imported_data['Sales Price']
        sale.delivery_date = parsed_date(
            date_format, imported_data['Delivery Date'])
        sale.finance_rate = imported_data['APR']
        sale.finance_term = imported_data['Term']
        sale.finance_amount = imported_data['Amount Financed']
        sale.date_of_inventory = parsed_date(
            date_format, imported_data['Inventory Date'])
        sale.has_service_contract = imported_data['Insurance Profit'] != ''

    if sale.sale_date and sale.date_of_inventory:
        sale.days_in_stock = (sale.sale_date - sale.date_of_inventory).days

    sale.dealer_integration_partner_id = dip_id
    return sale
