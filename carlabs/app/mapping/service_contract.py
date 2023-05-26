from orm.models.data_imports import DataImports
from orm.models.service_contract import ServiceContract
from utils import parsed_date

def map_service_contract(record: DataImports):
    service_contract = ServiceContract()
    imported_data = record.importedData
    if record.dataSource == 'CDK':
        date_format = '%Y-%m-%d'
        service_contract.contract_name = imported_data['Insurance1Name'][0]
        service_contract.start_date = parsed_date(date_format, imported_data['ContractDate'][0])
        service_contract.amount = imported_data['Ins1Income'][0]
        service_contract.cost = imported_data['Ins1Cost'][0]
        service_contract.deductible = imported_data['Insurance1Deductible'][0]
        service_contract.expiration_months = imported_data['Term'][0]
        service_contract.expiration_miles = imported_data['Insurance1LimitMiles'][0]
    elif record.dataSource == 'DealerTrack':
        date_format = '%Y%m%d'
        contracts = imported_data['deal']['detail']['ServiceContracts']
        if isinstance(contracts, list) and len(contracts) != 0:
            service_contract.contract_name = contracts[0]['ServiceContract']['ContractName']
            service_contract.start_date = parsed_date(date_format, contracts[0]['ServiceContract']['ContractStartDate'])
            service_contract.deductible = contracts[0]['ServiceContract']['ContractDeductible']
            service_contract.expiration_months = contracts[0]['ServiceContract']['ContractExpirationMonths']
            service_contract.expiration_miles = contracts[0]['ServiceContract']['ContractExpirationMiles']
        service_contract.amount = imported_data['deal']['detail']['ServiceContractAmount']
        service_contract.cost = imported_data['deal']['detail']['ServiceContractCost']
    elif record.dataSource == 'DEALERVAULT':
        date_format = '%Y%m%d'
        service_contract.contract_name = imported_data['Warranty 1 Name']
        service_contract.start_date = parsed_date(date_format, imported_data['Contract Date'])
        service_contract.amount = imported_data['Warranty 1 Sale']
        service_contract.cost = imported_data['Warranty 1 Cost']
        service_contract.expiration_months = imported_data['Warranty 1 Term']
        service_contract.expiration_miles = imported_data['Warranty 1 Miles']

    # TODO fix this
    service_contract.dealer_integration_partner_id = 1
    service_contract.contract_id = ''

    return service_contract