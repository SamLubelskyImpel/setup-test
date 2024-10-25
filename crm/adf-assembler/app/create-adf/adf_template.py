BASE_ADF_TEMPLATE = """
<?xml version="1.0"?>
<?adf version="1.0"?>
<adf>
    <prospect>
        <id sequence="1" source="Impel">{lead_id}</id>
        <requestdate>{request_date}</requestdate>
        <vehicle>
            {vehicle}
        </vehicle>
        <customer>
            <contact>
                {customer_contact}
                <address>
                    {customer_address}
                </address>
            </contact>
            {customer}
        </customer>
        <vendor>
            <vendorname>Impel</vendorname>
            <contact>
                <name part="full">{vendor_full_name}</name>
            </contact>
        </vendor>
        <provider>
            <name part="full">Impel-Chat</name>
            <service>Omnichannel Experience Platform for Dealerships</service>
            <url>https://www.impel.ai</url>
            <email>support@impel.ai</email>
            <phone>844-384-6735</phone>
            <contact primarycontact="1">
                <name part="full">Support</name>
                <email>support@impel.ai</email>
                <phone type="voice" time="day">844-384-6735</phone>
                <address>
                <street line="1">344 S Warren St #200</street>
                <city>Syracuse</city>
                <regioncode>NY</regioncode>
                <postalcode>13202</postalcode>
                <country>US</country>
                </address>
            </contact>
        </provider>
    </prospect>
</adf>
"""

LEAD_DATA_TO_ADF_MAPPER = {
    "year": "year",
    "make": "make",
    "model": "model",
    "status": "status",
    "body_style": "bodystyle",
    "condition": "condition",
    "odometer_units": "PARAMETERS",
    "price": "PARAMETERS",
    "stock_num": "stock",
    "trim": "trim",
    "vin": "vin",
    "address": "street",
    "city": "city",
    "country": "country",
    "email": "email",
    "phone": "phone",
    "postal_code": "postalcode",
    "comment": "comments",
}

OEM_MAPPING = {
    "vehicle": {
        "status": ' status="{status_value}"',
        "interest": ' interest="{interest_value}"',
        "year": "<year>{year_value}</year>",
        "make": "<make>{make_value}</make>",
        "model": "<model>{model_value}</model>",
        "vin": "<vin>{vin_value}</vin>",
        "stock_number": "<stock>{stock_number_value}</stock>",
        "comments": "<comments>{comments_value}</comments>",
    },
    "customer": {
        "first_name": '<name part="first" type="individual">{first_name_value}</name>',
        "last_name": '<name part="last" type="individual">{last_name_value}</name>',
        "phone": "<phone><![CDATA[{phone_value}]]></phone>",
        "email": "<email>{email_value}</email>",
        "address": {
            "address": '<street line="1">{address_value}</street>',
            "city": "<city>{city_value}</city>",
            "country": "<country>{country_value}</country>",
            "postal_code": "<postalcode>{postal_code_value}</postalcode>",
        }
    },
    "vendor": {
        "id": '<id source="{oem_recipient} Dealer Code">{dealer_code}</id>',
        "vendorname": "<vendorname>{vendorname_value}</vendorname>",
    }
}

OEM_ADF_TEMPLATE = """
<adf>
    <prospect status="new">
        <id source="ActivEngage" sequence="1">{lead_id}</id>
        <id source="LeadSubProgram">New Chat Sales Lead</id>
        <id source="MediaType">WEB</id>
        <requestdate>{request_date}</requestdate>
        {vehicle_of_interest}
        {customer}
        {vendor}
        <provider>
            <name>Impel-Chat</name>
            <service>{service_value}</service>
        </provider>
    </prospect>
</adf>
"""