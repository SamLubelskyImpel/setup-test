STANDARD_ADF_FORMAT = """
<?ADF VERSION "1.0"?>
<?XML VERSION "1.0"?>
<adf>
	<prospect>
		<requestdate>{request_date}</requestdate>
		<vehicle>
			<year>{year}</year>
			<make>{make}</make>
			<model>{model}</model>
		</vehicle>
		<customer>
			<contact>
				<name part="full">{full_name}</name>
				{contact_type}
			</contact>
		</customer>
        {appointment}
		<vendor>
			<contact>
				<name part="full">{vendor_full_name}</name>
			</contact>
		</vendor>
	</prospect>
</adf>
"""

APPOINTMENT_ADF = """
<comment>
	{activity_time}
</comment>
"""