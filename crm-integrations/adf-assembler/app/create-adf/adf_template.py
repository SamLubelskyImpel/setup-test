BASE_ADF_TEMPLATE = """
<?ADF VERSION "1.0"?>
<?XML VERSION "1.0"?>
<adf>
	<prospect>
        <id sequence=1 source="Impel">{lead_id}</id>
		<requestdate>{request_date}</requestdate>
		<vehicle>
			{vehicle}
		</vehicle>
		<customer>
			{customer}
		</customer>
		<vendor>
			{vendor}
			<contact>
				<name part="full">{vendor_full_name}</name>
			</contact>
		</vendor>
	</prospect>
</adf>
"""

LEAD_DATA_TO_ADF_MAPPER = {
    "year": "year",
    "make": "make",
    "model": "model",
    "address": "address",
    "city": "city",
    "country": "country",
    "email": "email",
    "phone": "phone",
    "postal_code": "postalcode",
    "comment": "comment",
    "status": "status",
    "body_style": "bodystyle",
    "condition": "condition",
    "odometer_units": "PARAMETERS",
    "price": "PARAMETERS",
    "stock_num": "stock",
    "trim": "trim",
    "vin": "vin",
}
