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


HONDA_ADF_TEMPLATE = """
<adf>
    <prospect status="new">
        <id source="ActivEngage" sequence="1">18014964</id>
        <id source="IPAddress">172.124.188.247</id>
        <id source="UserAgent">Mozilla/5.0 (iPhone; CPU iPhone OS 15_2 like MacOS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Mobile/15E148Safari/604.1</id>
        <id source="URL">https://www.honda.com/inventory/new-2022-honda-Accord-4dr-car-kmhll4xxaa289858/</id>
        <id source="TPCA_Consent">NULL</id>
        <id source="LeadSubProgram">New Chat Sales Lead</id>
        <id source="MediaType">WEB</id>
        <requestdate>2022-03-15T16:55:23+00:00</requestdate>
        <vehicle status="new" interest="buy">
            <make>Honda</make>
            <year>2022</year>
            <model>Accord</model>
            <vin> JHLRW2H59KX00000</vin>
            <stock></stock>
            <comments></comments>
        </vehicle>
        <customer>
            <contact>
            <name part="first" type="individual">Hailees</name>
            <name part="last" type="individual">Comet</name>
            <email>haileescomet2@gmail.com</email>
            <phone type="voice" time="day"><![CDATA[516-504-0006]]></phone>
            <address>
            <street line="1"></street>
            <city></city>
            <regioncode></regioncode>
            <postalcode>77373</postalcode>
            </address>
            </contact>
            <timeframe>
            <description>2 weeks</description>
            </timeframe>
            <comments>Chat Transcript without any PCI data.</comments>
        </customer>
        <vendor>
            <id source=" Honda Dealer Code">208625</id>
            <vendorname>Lou Sobh Honda</vendorname>
        </vendor>
        <provider>
            <name>ActivEngage - Chat</name>
            <service>90534</service>
            <url>https://conversations.activEngage.com/</url>
        </provider>
    </prospect>
</adf>
"""