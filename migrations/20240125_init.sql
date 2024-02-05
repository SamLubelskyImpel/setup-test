-- prod.crm_activity_type definition

-- Drop table

-- DROP TABLE prod.crm_activity_type;

CREATE TABLE prod.crm_activity_type (
	id serial4 NOT NULL,
	"type" varchar(40) NOT NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_activity_type PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_activity_type for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_activity_type for each row execute function update_db_update_date_and_role();


-- prod.crm_dealer definition

-- Drop table

-- DROP TABLE prod.crm_dealer;

CREATE TABLE prod.crm_dealer (
	id serial4 NOT NULL,
	product_dealer_id varchar(40) NOT NULL,
	sfdc_account_id varchar(40) NOT NULL,
	dealer_name varchar(80) NULL,
	dealer_location_name varchar(80) NULL,
	country varchar(40) NULL,
	state varchar(20) NULL,
	city varchar(40) NULL,
	zip_code varchar(10) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_dealer PRIMARY KEY (id),
	CONSTRAINT uq_product_dealer_id UNIQUE (product_dealer_id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_dealer for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_dealer for each row execute function update_db_update_date_and_role();


-- prod.crm_integration_partner definition

-- Drop table

-- DROP TABLE prod.crm_integration_partner;

CREATE TABLE prod.crm_integration_partner (
	id serial4 NOT NULL,
	impel_integration_partner_name varchar(40) NOT NULL,
	integration_date timestamptz NOT NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_integration_partner PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_integration_partner for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_integration_partner for each row execute function update_db_update_date_and_role();


-- prod.dealer_group definition

-- Drop table

-- DROP TABLE prod.dealer_group;

CREATE TABLE prod.dealer_group (
	id serial4 NOT NULL,
	impel_dealer_group_id varchar(80) NOT NULL,
	duns_no varchar(20) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	db_update_date timestamp NULL,
	db_update_user varchar(255) NULL,
	CONSTRAINT dealer_group_name_key UNIQUE (impel_dealer_group_id),
	CONSTRAINT dealer_group_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_update_db_update_date_and_user_dealer_group before
update
    on
    prod.dealer_group for each row execute function update_db_update_date_and_user_dealer_group();


-- prod.integration_partner definition

-- Drop table

-- DROP TABLE prod.integration_partner;

CREATE TABLE prod.integration_partner (
	id serial4 NOT NULL,
	impel_integration_partner_id varchar(40) NOT NULL,
	"type" varchar(20) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	db_update_date timestamp NULL,
	db_update_user varchar(255) NULL,
	CONSTRAINT integration_partner_pkey PRIMARY KEY (id),
	CONSTRAINT unique_integration_partner_impel_id UNIQUE (impel_integration_partner_id)
);

-- Table Triggers

create trigger tr_update_db_update_date_and_user_integration_partner before
update
    on
    prod.integration_partner for each row execute function update_db_update_date_and_user_integration_partner();


-- prod.sfdc_account definition

-- Drop table

-- DROP TABLE prod.sfdc_account;

CREATE TABLE prod.sfdc_account (
	id serial4 NOT NULL,
	sfdc_account_id varchar(80) NOT NULL,
	customer_type varchar(40) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	db_update_date timestamp NULL,
	db_update_user varchar(255) NULL,
	CONSTRAINT sfdc_account_pkey PRIMARY KEY (id),
	CONSTRAINT sfdc_account_sfdc_account_id_key UNIQUE (sfdc_account_id)
);

-- Table Triggers

create trigger tr_update_db_update_date_and_user_sfdc_account before
update
    on
    prod.sfdc_account for each row execute function update_db_update_date_and_user_sfdc_account();


-- prod.crm_dealer_integration_partner definition

-- Drop table

-- DROP TABLE prod.crm_dealer_integration_partner;

CREATE TABLE prod.crm_dealer_integration_partner (
	id serial4 NOT NULL,
	crm_dealer_id varchar(40) NULL,
	integration_partner_id int4 NOT NULL,
	dealer_id int4 NOT NULL,
	is_active bool NULL DEFAULT false,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_dealer_integration_partner PRIMARY KEY (id),
	CONSTRAINT fk_crm_dealer_integration_partner FOREIGN KEY (integration_partner_id) REFERENCES prod.crm_integration_partner(id),
	CONSTRAINT fk_crm_dealer_integration_partner_dealer FOREIGN KEY (dealer_id) REFERENCES prod.crm_dealer(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_dealer_integration_partner for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_dealer_integration_partner for each row execute function update_db_update_date_and_role();


-- prod.crm_salesperson definition

-- Drop table

-- DROP TABLE prod.crm_salesperson;

CREATE TABLE prod.crm_salesperson (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	crm_salesperson_id varchar(100) NULL,
	first_name varchar(40) NULL,
	last_name varchar(40) NULL,
	email varchar(40) NULL,
	phone varchar(15) NULL,
	position_name varchar(40) NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_salesperson PRIMARY KEY (id),
	CONSTRAINT fk_crm_salesperson_dealer_integration_partner FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.crm_dealer_integration_partner(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_salesperson for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_salesperson for each row execute function update_db_update_date_and_role();


-- prod.dealer definition

-- Drop table

-- DROP TABLE prod.dealer;

CREATE TABLE prod.dealer (
	id serial4 NOT NULL,
	dealer_group_id int4 NULL,
	impel_dealer_id varchar(100) NOT NULL,
	location_name varchar(80) NULL,
	state varchar(20) NULL,
	city varchar(40) NULL,
	zip_code varchar(20) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	sfdc_account_id int4 NULL,
	db_update_date timestamp NULL,
	db_update_user varchar(255) NULL,
	full_name varchar(255) NULL,
	CONSTRAINT dealer_pkey PRIMARY KEY (id),
	CONSTRAINT unique_impel_id UNIQUE (impel_dealer_id),
	CONSTRAINT dealer_dealer_group_id_fkey FOREIGN KEY (dealer_group_id) REFERENCES prod.dealer_group(id),
	CONSTRAINT dealer_sfdc_account_id_fkey FOREIGN KEY (sfdc_account_id) REFERENCES prod.sfdc_account(id)
);

-- Table Triggers

create trigger tr_update_db_update_date_and_user_dealer before
update
    on
    prod.dealer for each row execute function update_db_update_date_and_user_dealer();


-- prod.dealer_integration_partner definition

-- Drop table

-- DROP TABLE prod.dealer_integration_partner;

CREATE TABLE prod.dealer_integration_partner (
	id serial4 NOT NULL,
	integration_partner_id int8 NOT NULL,
	dealer_id int8 NOT NULL,
	dms_id varchar(255) NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	is_vehicle_sale_integration bool NULL,
	is_repair_order_integration bool NULL,
	is_service_contract_integration bool NULL,
	is_appointment_integration bool NULL,
	is_customer_integration bool NULL,
	db_update_date timestamp NULL,
	db_update_user varchar(255) NULL,
	CONSTRAINT dealer_integration_partner_pkey PRIMARY KEY (id),
	CONSTRAINT dealer_integration_partner_un UNIQUE (dealer_id, integration_partner_id, dms_id),
	CONSTRAINT dealer_integration_partner_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES prod.dealer(id),
	CONSTRAINT dealer_integration_partner_integration_id_fkey FOREIGN KEY (integration_partner_id) REFERENCES prod.integration_partner(id)
);

-- Table Triggers

create trigger tr_update_db_update_date_and_user_dealer_integration_partner before
update
    on
    prod.dealer_integration_partner for each row execute function update_db_update_date_and_user_dealer_integration_partner();


-- prod.op_code definition

-- Drop table

-- DROP TABLE prod.op_code;

CREATE TABLE prod.op_code (
	id serial4 NOT NULL,
	dealer_integration_partner_id int8 NOT NULL,
	op_code varchar(255) NULL,
	op_code_desc varchar(305) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT op_code_pkey PRIMARY KEY (id),
	CONSTRAINT unique_op_code UNIQUE (dealer_integration_partner_id, op_code, op_code_desc),
	CONSTRAINT op_code_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id)
);


-- prod.vehicle definition

-- Drop table

-- DROP TABLE prod.vehicle;

CREATE TABLE prod.vehicle (
	id serial4 NOT NULL,
	vin varchar NULL,
	oem_name varchar(80) NULL,
	"type" varchar(45) NULL,
	vehicle_class varchar(40) NULL,
	mileage int4 NULL,
	make varchar(80) NULL,
	model varchar(80) NULL,
	"year" int4 NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	dealer_integration_partner_id int4 NULL,
	new_or_used varchar(1) NULL,
	metadata jsonb NULL,
	stock_num varchar(40) NULL,
	warranty_expiration_miles int8 NULL,
	warranty_expiration_date timestamp NULL,
	CONSTRAINT vehicle_pkey PRIMARY KEY (id),
	CONSTRAINT vehicle_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id)
);


-- prod.consumer definition

-- Drop table

-- DROP TABLE prod.consumer;

CREATE TABLE prod.consumer (
	id serial4 NOT NULL,
	dealer_integration_partner_id int8 NOT NULL,
	dealer_customer_no varchar(40) NULL,
	first_name varchar NULL,
	last_name varchar NULL,
	email varchar(80) NULL,
	ip_address varchar(20) NULL,
	cell_phone varchar NULL,
	city varchar(40) NULL,
	state varchar(40) NULL,
	metro varchar(80) NULL,
	postal_code varchar NULL,
	home_phone varchar NULL,
	email_optin_flag bool NULL,
	phone_optin_flag bool NULL,
	postal_mail_optin_flag bool NULL,
	sms_optin_flag bool NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	metadata jsonb NULL,
	master_consumer_id varchar(40) NULL,
	address varchar(100) NULL,
	CONSTRAINT consumer_pkey PRIMARY KEY (id),
	CONSTRAINT consumer_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id)
);


-- prod.crm_consumer definition

-- Drop table

-- DROP TABLE prod.crm_consumer;

CREATE TABLE prod.crm_consumer (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	crm_consumer_id varchar(40) NULL,
	first_name varchar(40) NULL,
	last_name varchar(40) NULL,
	middle_name varchar(40) NULL,
	email varchar(40) NULL,
	phone varchar(15) NULL,
	postal_code varchar(10) NULL,
	address varchar(255) NULL,
	country varchar(40) NULL,
	city varchar(40) NULL,
	email_optin_flag bool NULL DEFAULT true,
	sms_optin_flag bool NULL DEFAULT true,
	request_product varchar(20) NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_consumer PRIMARY KEY (id),
	CONSTRAINT uq_consumer UNIQUE (crm_consumer_id, dealer_integration_partner_id),
	CONSTRAINT fk_crm_consumer_dealer_integration_partner FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.crm_dealer_integration_partner(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_consumer for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_consumer for each row execute function update_db_update_date_and_role();


-- prod.crm_lead definition

-- Drop table

-- DROP TABLE prod.crm_lead;

CREATE TABLE prod.crm_lead (
	id serial4 NOT NULL,
	consumer_id int4 NOT NULL,
	crm_lead_id varchar(40) NULL,
	lead_ts timestamptz NULL,
	status varchar(40) NULL,
	substatus varchar(20) NULL,
	"comment" varchar(256) NULL,
	origin_channel varchar(20) NULL,
	source_channel varchar(40) NULL,
	request_product varchar(20) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_lead PRIMARY KEY (id),
	CONSTRAINT uq_lead UNIQUE (consumer_id, crm_lead_id),
	CONSTRAINT fk_crm_consumer FOREIGN KEY (consumer_id) REFERENCES prod.crm_consumer(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_lead for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_lead for each row execute function update_db_update_date_and_role();


-- prod.crm_lead_salesperson definition

-- Drop table

-- DROP TABLE prod.crm_lead_salesperson;

CREATE TABLE prod.crm_lead_salesperson (
	id serial4 NOT NULL,
	lead_id int4 NOT NULL,
	salesperson_id int4 NOT NULL,
	is_primary bool NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_lead_salesperson PRIMARY KEY (id),
	CONSTRAINT fk_crm_lead_lead_salesperson FOREIGN KEY (lead_id) REFERENCES prod.crm_lead(id),
	CONSTRAINT fk_crm_salesperson_lead_salesperson FOREIGN KEY (salesperson_id) REFERENCES prod.crm_salesperson(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_lead_salesperson for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_lead_salesperson for each row execute function update_db_update_date_and_role();


-- prod.crm_vehicle definition

-- Drop table

-- DROP TABLE prod.crm_vehicle;

CREATE TABLE prod.crm_vehicle (
	id serial4 NOT NULL,
	lead_id int4 NOT NULL,
	vin varchar(20) NOT NULL,
	crm_vehicle_id varchar(40) NULL,
	stock_num varchar(40) NULL,
	oem_name varchar(80) NULL,
	"type" varchar(20) NULL,
	vehicle_class varchar(40) NULL,
	mileage int4 NULL,
	make varchar(80) NULL,
	model varchar(80) NULL,
	manufactured_year int4 NULL,
	body_style varchar(20) NULL,
	transmission varchar(40) NULL,
	interior_color varchar(40) NULL,
	exterior_color varchar(40) NULL,
	trim varchar(20) NULL,
	price numeric(10, 2) NULL,
	status varchar(40) NULL,
	"condition" varchar(5) NULL,
	odometer_units varchar(20) NULL,
	vehicle_comments varchar(80) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_vehicle PRIMARY KEY (id),
	CONSTRAINT fk_crm_vehicle_lead FOREIGN KEY (lead_id) REFERENCES prod.crm_lead(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_vehicle for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_vehicle for each row execute function update_db_update_date_and_role();


-- prod.inventory definition

-- Drop table

-- DROP TABLE prod.inventory;

CREATE TABLE prod.inventory (
	id serial4 NOT NULL,
	vehicle_id int4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	upload_date timestamp NULL,
	list_price float8 NULL,
	msrp float8 NULL,
	invoice_price float8 NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT inventory_pkey PRIMARY KEY (id),
	CONSTRAINT unique_inventory UNIQUE (vehicle_id, dealer_integration_partner_id),
	CONSTRAINT inventory_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id),
	CONSTRAINT inventory_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES prod.vehicle(id)
);


-- prod.service_repair_order definition

-- Drop table

-- DROP TABLE prod.service_repair_order;

CREATE TABLE prod.service_repair_order (
	id serial4 NOT NULL,
	dealer_integration_partner_id int8 NOT NULL,
	consumer_id int8 NOT NULL,
	vehicle_id int8 NULL,
	ro_open_date timestamp NOT NULL,
	ro_close_date timestamp NULL,
	txn_pay_type varchar NULL,
	repair_order_no varchar(30) NULL,
	advisor_name varchar(40) NULL,
	total_amount float8 NULL,
	consumer_total_amount float8 NULL,
	warranty_total_amount float8 NULL,
	"comment" text NULL,
	recommendation text NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	metadata jsonb NULL,
	internal_total_amount float8 NULL,
	db_update_date timestamp NULL,
	db_update_role varchar(30) NULL,
	CONSTRAINT service_repair_order_pkey PRIMARY KEY (id),
	CONSTRAINT unique_ros_dms UNIQUE (repair_order_no, dealer_integration_partner_id),
	CONSTRAINT service_repair_order_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES prod.consumer(id),
	CONSTRAINT service_repair_order_dealer_integration_partner_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id),
	CONSTRAINT service_repair_order_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES prod.vehicle(id)
);
CREATE INDEX idx_ro_close_date ON prod.service_repair_order USING btree (ro_close_date);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.service_repair_order for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.service_repair_order for each row execute function update_db_update_date_and_role();


-- prod.vehicle_sale definition

-- Drop table

-- DROP TABLE prod.vehicle_sale;

CREATE TABLE prod.vehicle_sale (
	id serial4 NOT NULL,
	dealer_integration_partner_id int8 NOT NULL,
	vehicle_id int8 NULL,
	consumer_id int8 NOT NULL,
	sale_date timestamp NULL,
	listed_price float8 NULL,
	sales_tax float8 NULL,
	mileage_on_vehicle int8 NULL,
	deal_type varchar NULL,
	cost_of_vehicle float8 NULL,
	oem_msrp float8 NULL,
	adjustment_on_price float8 NULL,
	days_in_stock int8 NULL,
	date_of_state_inspection date NULL,
	trade_in_value float8 NULL,
	payoff_on_trade float8 NULL,
	value_at_end_of_lease float8 NULL,
	miles_per_year int8 NULL,
	profit_on_sale float8 NULL,
	has_service_contract bool NULL,
	vehicle_gross float8 NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	vin varchar NULL,
	delivery_date timestamp NULL,
	metadata jsonb NULL,
	finance_rate varchar(40) NULL,
	finance_term varchar(40) NULL,
	finance_amount varchar(40) NULL,
	date_of_inventory timestamp NULL,
	db_update_date timestamp NULL,
	db_update_role varchar(30) NULL,
	CONSTRAINT unique_vehicle_sale UNIQUE (dealer_integration_partner_id, sale_date, vin),
	CONSTRAINT vehicle_sale_pkey PRIMARY KEY (id),
	CONSTRAINT vehicle_sale_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES prod.consumer(id),
	CONSTRAINT vehicle_sale_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id),
	CONSTRAINT vehicle_sale_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES prod.vehicle(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.vehicle_sale for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.vehicle_sale for each row execute function update_db_update_date_and_role();


-- prod.appointment definition

-- Drop table

-- DROP TABLE prod.appointment;

CREATE TABLE prod.appointment (
	id serial4 NOT NULL,
	dealer_integration_partner_id int8 NOT NULL,
	consumer_id int8 NOT NULL,
	vehicle_id int8 NOT NULL,
	appointment_time time NULL,
	appointment_date date NULL,
	appointment_source varchar(100) NULL,
	reason_code varchar(100) NULL,
	appointment_create_ts timestamp NULL,
	appointment_update_ts timestamp NULL,
	rescheduled_flag bool NULL,
	appointment_no varchar NULL,
	last_ro_date date NULL,
	last_ro_num varchar NULL,
	db_creation_date timestamp NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'::text),
	db_update_date timestamp NULL,
	db_update_role varchar(30) NULL,
	metadata jsonb NULL,
	converted_ro_num varchar(30) NULL,
	CONSTRAINT appointment_pkey PRIMARY KEY (id),
	CONSTRAINT unique_appointment UNIQUE (dealer_integration_partner_id, appointment_no),
	CONSTRAINT consumer_appointment_fkey FOREIGN KEY (consumer_id) REFERENCES prod.consumer(id),
	CONSTRAINT dip_appointment_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id),
	CONSTRAINT vehicle_appointment_fkey FOREIGN KEY (vehicle_id) REFERENCES prod.vehicle(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appointment for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appointment for each row execute function update_db_update_date_and_role();


-- prod.crm_activity definition

-- Drop table

-- DROP TABLE prod.crm_activity;

CREATE TABLE prod.crm_activity (
	id serial4 NOT NULL,
	lead_id int4 NOT NULL,
	activity_type_id int4 NOT NULL,
	crm_activity_id varchar(40) NULL,
	activity_requested_ts timestamptz NULL,
	request_product varchar(20) NULL,
	metadata jsonb NULL,
	notes varchar NULL,
	activity_due_ts timestamptz NULL,
	db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_activity PRIMARY KEY (id),
	CONSTRAINT fk_activity_lead FOREIGN KEY (lead_id) REFERENCES prod.crm_lead(id),
	CONSTRAINT fk_activity_type FOREIGN KEY (activity_type_id) REFERENCES prod.crm_activity_type(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_activity for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_activity for each row execute function update_db_update_date_and_role();


-- prod.op_code_appointment definition

-- Drop table

-- DROP TABLE prod.op_code_appointment;

CREATE TABLE prod.op_code_appointment (
	id serial4 NOT NULL,
	op_code_id int8 NOT NULL,
	appointment_id int8 NOT NULL,
	CONSTRAINT op_code_appointment_pkey PRIMARY KEY (id),
	CONSTRAINT unique_op_code_appointment UNIQUE (op_code_id, appointment_id),
	CONSTRAINT op_code_appointment_appointment_id_fkey FOREIGN KEY (appointment_id) REFERENCES prod.appointment(id) ON DELETE CASCADE,
	CONSTRAINT op_code_appointment_op_code_id_fkey FOREIGN KEY (op_code_id) REFERENCES prod.op_code(id)
);


-- prod.op_code_repair_order definition

-- Drop table

-- DROP TABLE prod.op_code_repair_order;

CREATE TABLE prod.op_code_repair_order (
	id serial4 NOT NULL,
	op_code_id int8 NOT NULL,
	repair_order_id int8 NOT NULL,
	CONSTRAINT op_code_repair_order_pkey PRIMARY KEY (id),
	CONSTRAINT unique_op_code_repair_order UNIQUE (op_code_id, repair_order_id),
	CONSTRAINT op_code_repair_order_op_code_id_fkey FOREIGN KEY (op_code_id) REFERENCES prod.op_code(id),
	CONSTRAINT op_code_repair_order_repair_order_id_fkey FOREIGN KEY (repair_order_id) REFERENCES prod.service_repair_order(id) ON DELETE CASCADE
);


-- prod.service_contracts definition

-- Drop table

-- DROP TABLE prod.service_contracts;

CREATE TABLE prod.service_contracts (
	id serial4 NOT NULL,
	dealer_integration_partner_id int8 NOT NULL,
	contract_id varchar(40) NULL,
	contract_name varchar(80) NULL,
	start_date timestamp NULL,
	amount varchar(40) NULL,
	"cost" varchar(40) NULL,
	deductible varchar(40) NULL,
	expiration_months varchar(40) NULL,
	expiration_miles varchar(40) NULL,
	db_creation_date timestamp NOT NULL DEFAULT now(),
	warranty_expiration_date date NULL,
	extended_warranty jsonb NULL,
	service_package_flag bool NULL,
	vehicle_sale_id int8 NULL,
	appointment_id int8 NULL,
	CONSTRAINT service_contracts_pkey PRIMARY KEY (id),
	CONSTRAINT unique_service_contracts UNIQUE NULLS NOT DISTINCT (dealer_integration_partner_id, contract_id, vehicle_sale_id, appointment_id),
	CONSTRAINT service_contracts_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.dealer_integration_partner(id),
	CONSTRAINT service_contracts_vehicle_sale_id_fkey FOREIGN KEY (vehicle_sale_id) REFERENCES prod.vehicle_sale(id)
);
