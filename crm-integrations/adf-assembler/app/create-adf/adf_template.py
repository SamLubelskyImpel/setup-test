BASE_ADF_TEMPLATE = """
<?adf version="1.0"?>
<?xml version="1.0"?>
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
