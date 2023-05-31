from orm.models.data_imports import DataImports
from orm.models.consumer import Consumer


def map_consumer(record: DataImports, dip_id: int):
    consumer = Consumer()
    imported_data = record.importedData
    if record.dataSource == 'CDK':
        if isinstance(imported_data['user'], dict):
            consumer.first_name = imported_data['user']['firstName']
            consumer.last_name = imported_data['user']['lastName']
            consumer.cell_phone = imported_data['user']['sms']
            never_contact = imported_data['user']['doNotContact']['ever']
            if not never_contact:
                consumer.sms_optin_flag = not imported_data['user']['doNotContact']['sms']
                consumer.email_optin_flag = not imported_data['user']['doNotContact']['email']
                consumer.phone_optin_flag = True
                consumer.postal_mail_optin_flag = True
        consumer.email = imported_data['Email1'][0]
        consumer.state = imported_data['State'][0]
        consumer.postal_code = imported_data['ZipOrPostalCode'][0]
        consumer.home_phone = imported_data['HomePhone'][0]
    elif record.dataSource == 'DealerTrack':
        consumer.dealer_customer_no = imported_data['BuyerCustomerNumber']
        consumer.first_name = imported_data['BuyerFirstName']
        consumer.last_name = imported_data['BuyerLastName']
        consumer.email = imported_data['customerInformation']['Email1']
        consumer.cell_phone = imported_data['customerInformation']['CellPhone']
        consumer.city = imported_data['customerInformation']['City']
        consumer.state = imported_data['customerInformation']['StateCode']
        consumer.postal_code = imported_data['customerInformation']['ZipCode']
        consumer.home_phone = imported_data['customerInformation']['PhoneNumber']
        consumer.email_optin_flag = imported_data['customerInformation']['AllowContactByEmail'] == 'Y'
        consumer.phone_optin_flag = imported_data['customerInformation']['AllowContactByPhone'] == 'Y'
        consumer.postal_mail_optin_flag = imported_data[
            'customerInformation']['AllowContactByPostal'] == 'Y'
        consumer.sms_optin_flag = imported_data['customerInformation']['AllowContactByPhone'] == 'Y'
    elif record.dataSource == 'DEALERVAULT':
        consumer.dealer_customer_no = imported_data['Customer Number']
        consumer.first_name = imported_data['First Name']
        consumer.last_name = imported_data['Last Name']
        consumer.email = imported_data['Email 1']
        consumer.cell_phone = imported_data['Cell Phone']
        consumer.city = imported_data['City']
        consumer.state = imported_data['State']
        consumer.postal_code = imported_data['Zip']
        consumer.home_phone = imported_data['Home Phone']
        consumer.email_optin_flag = imported_data['Block Email'] == 'N'
        consumer.phone_optin_flag = imported_data['Block Phone'] == 'N'
        consumer.postal_mail_optin_flag = imported_data['Block Mail'] == 'N'
        consumer.sms_optin_flag = imported_data['Block Phone'] == 'N'

    consumer.dealer_integration_partner_id = dip_id
    consumer.metadata_column = {'data_imports_id': record.id, 'data_source': record.dataSource}

    return consumer
