-- stage.dealer_group definition

-- Drop table

-- DROP TABLE stage.dealer_group;

CREATE TABLE stage.dealer_group (
	id serial4 NOT NULL,
	"name" varchar(80) NOT NULL,
	duns_no varchar(20) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT dealer_group_name_key UNIQUE (name),
	CONSTRAINT dealer_group_pkey PRIMARY KEY (id)
);

-- stage.integration_partner definition

-- Drop table

-- DROP TABLE stage.integration_partner;

CREATE TABLE stage.integration_partner (
	id serial4 NOT NULL,
	"name" varchar(40) NOT NULL,
	"type" varchar(20) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT integration_partner_pkey PRIMARY KEY (id)
);


-- stage.sfdc_account definition

-- Drop table

-- DROP TABLE stage.sfdc_account;

CREATE TABLE stage.sfdc_account (
	id serial4 NOT NULL,
	sfdc_account_id varchar(80) NOT NULL,
	customer_type varchar(40) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT sfdc_account_pkey PRIMARY KEY (id),
	CONSTRAINT sfdc_account_sfdc_account_id_key UNIQUE (sfdc_account_id)
);


-- stage.vehicle definition

-- Drop table

-- DROP TABLE stage.vehicle;

CREATE TABLE stage.vehicle (
	id serial4 NOT NULL,
	vin varchar(40) NOT NULL,
	oem_name varchar(80) NULL,
	"type" varchar(20) NULL,
	vehicle_class varchar(40) NULL,
	mileage int4 NULL,
	make varchar(80) NULL,
	model varchar(80) NULL,
	"year" int4 NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT vehicle_pkey PRIMARY KEY (id)
);


-- stage.dealer definition

-- Drop table

-- DROP TABLE stage.dealer;

CREATE TABLE stage.dealer (
	id serial4 NOT NULL,
	sfdc_account_id int4 NOT NULL,
	dealer_group_id int4 NOT NULL,
	"name" varchar(80) NOT NULL,
	location_name varchar(80) NULL,
	state varchar(20) NULL,
	city varchar(40) NULL,
	zip_code varchar(20) NULL,
	category varchar(40) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT dealer_pkey PRIMARY KEY (id),
	CONSTRAINT dealer_dealer_group_id_fkey FOREIGN KEY (dealer_group_id) REFERENCES stage.dealer_group(id),
	CONSTRAINT dealer_sfdc_account_id_fkey FOREIGN KEY (sfdc_account_id) REFERENCES stage.sfdc_account(id)
);


-- stage.inventory definition

-- Drop table

-- DROP TABLE stage.inventory;

CREATE TABLE stage.inventory (
	id serial4 NOT NULL,
	vehicle_id int4 NOT NULL,
	dealer_id int4 NOT NULL,
	upload_date timestamp NULL,
	list_price float8 NULL,
	msrp float8 NULL,
	invoice_price float8 NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT inventory_pkey PRIMARY KEY (id),
	CONSTRAINT unique_inventory UNIQUE (vehicle_id, dealer_id),
	CONSTRAINT inventory_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES stage.dealer(id),
	CONSTRAINT inventory_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES stage.vehicle(id)
);


-- stage.op_code definition

-- Drop table

-- DROP TABLE stage.op_code;

CREATE TABLE stage.op_code (
	id serial4 NOT NULL,
	dealer_id int8 NOT NULL,
	op_code varchar(255) NULL,
	op_code_desc varchar(255) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT op_code_pkey PRIMARY KEY (id),
	CONSTRAINT unique_op_code UNIQUE (dealer_id, op_code, op_code_desc),
	CONSTRAINT op_code_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES stage.dealer(id)
);


-- stage.consumer definition

-- Drop table

-- DROP TABLE stage.consumer;

CREATE TABLE stage.consumer (
	id serial4 NOT NULL,
	dealer_id int8 NOT NULL,
	dealer_customer_no varchar(40) NULL,
	first_name varchar(50) NULL,
	last_name varchar(50) NULL,
	email varchar(80) NULL,
	ip_address varchar(20) NULL,
	cell_phone varchar(15) NULL,
	city varchar(40) NULL,
	state varchar(3) NULL,
	metro varchar(80) NULL,
	postal_code varchar(10) NULL,
	home_phone varchar(20) NULL,
	email_optin_flag bool NULL,
	phone_optin_flag bool NULL,
	postal_mail_optin_flag bool NULL,
	sms_optin_flag bool NULL,
	master_consumer_id int8 NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT consumer_pkey PRIMARY KEY (id),
	CONSTRAINT consumer_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES stage.dealer(id)
);


-- stage.service_repair_order definition

-- Drop table

-- DROP TABLE stage.service_repair_order;

CREATE TABLE stage.service_repair_order (
	id serial4 NOT NULL,
	partner_id int8 NOT NULL,
	dealer_id int8 NOT NULL,
	consumer_id int8 NOT NULL,
	vehicle_id int8 NOT NULL,
	ro_open_date timestamp NOT NULL,
	ro_close_date timestamp NULL,
	txn_pay_type varchar(20) NULL,
	repair_order_no varchar(30) NULL,
	advisor_name varchar(40) NULL,
	total_amount float8 NULL,
	consumer_total_amount float8 NULL,
	warranty_total_amount float8 NULL,
	"comment" varchar(255) NULL,
	recommendation varchar(255) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT service_repair_order_pkey PRIMARY KEY (id),
	CONSTRAINT unique_ros_dms UNIQUE (partner_id, dealer_id, repair_order_no),
	CONSTRAINT service_repair_order_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES stage.consumer(id),
	CONSTRAINT service_repair_order_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES stage.dealer(id),
	CONSTRAINT service_repair_order_partner_id_fkey FOREIGN KEY (partner_id) REFERENCES stage.integration_partner(id),
	CONSTRAINT service_repair_order_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES stage.vehicle(id)
);


-- stage.vehicle_sale definition

-- Drop table

-- DROP TABLE stage.vehicle_sale;

CREATE TABLE stage.vehicle_sale (
	id serial4 NOT NULL,
	dealer_id int8 NOT NULL,
	vehicle_id int8 NOT NULL,
	consumer_id int8 NOT NULL,
	sale_date timestamp NULL,
	listed_price float8 NULL,
	sales_tax float8 NULL,
	mileage_on_vehicle int8 NULL,
	deal_type varchar(20) NULL,
	cost_of_vehicle float8 NULL,
	oem_msrp float8 NULL,
	discount_on_price float8 NULL,
	days_in_stock int8 NULL,
	date_of_state_inspection date NULL,
	is_new bool NULL,
	trade_in_value float8 NULL,
	payoff_on_trade float8 NULL,
	value_at_end_of_lease float8 NULL,
	miles_per_year int8 NULL,
	profit_on_sale float8 NULL,
	has_service_contract bool NULL,
	vehicle_gross float8 NULL,
	warranty_expiration_date date NULL,
	service_package jsonb NULL,
	extended_warranty jsonb NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT unique_vehicle_sale UNIQUE (dealer_id, vehicle_id, consumer_id),
	CONSTRAINT vehicle_sale_pkey PRIMARY KEY (id),
	CONSTRAINT vehicle_sale_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES stage.consumer(id),
	CONSTRAINT vehicle_sale_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES stage.dealer(id),
	CONSTRAINT vehicle_sale_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES stage.vehicle(id)
);


-- stage.op_code_vehicle_sale definition

-- Drop table

-- DROP TABLE stage.op_code_vehicle_sale;

CREATE TABLE stage.op_code_vehicle_sale (
	id serial4 NOT NULL,
	op_code_id int8 NOT NULL,
	vehicle_sale_id int8 NOT NULL,
	CONSTRAINT op_code_vehicle_sale_pkey PRIMARY KEY (id),
	CONSTRAINT op_code_vehicle_sale_op_code_id_fkey FOREIGN KEY (op_code_id) REFERENCES stage.op_code(id),
	CONSTRAINT op_code_vehicle_sale_vehicle_sale_id_fkey FOREIGN KEY (vehicle_sale_id) REFERENCES stage.vehicle_sale(id)
);