-- prod.crm_activity_type definition

-- Drop table

-- DROP TABLE prod.crm_activity_type;

CREATE TABLE prod.crm_activity_type (
	id serial4 NOT NULL,
	"type" varchar(40) NOT NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_activity_type PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_activity_type for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_activity_type for each row execute function public.update_db_update_date_and_role();


-- prod.crm_dealer definition

-- Drop table

-- DROP TABLE prod.crm_dealer;

CREATE TABLE prod.crm_dealer (
	id serial4 NOT NULL,
	product_dealer_id varchar(100) NOT NULL,
	sfdc_account_id varchar(40) NOT NULL,
	dealer_name varchar(80) NULL,
	dealer_location_name varchar(80) NULL,
	country varchar(40) NULL,
	state varchar(20) NULL,
	city varchar(40) NULL,
	zip_code varchar(10) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_dealer PRIMARY KEY (id),
	CONSTRAINT uq_product_dealer_id UNIQUE (product_dealer_id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_dealer for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_dealer for each row execute function public.update_db_update_date_and_role();


-- prod.crm_integration_partner definition

-- Drop table

-- DROP TABLE prod.crm_integration_partner;

CREATE TABLE prod.crm_integration_partner (
	id serial4 NOT NULL,
	impel_integration_partner_name varchar(40) NOT NULL,
	integration_date timestamptz NOT NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_integration_partner PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_integration_partner for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_integration_partner for each row execute function public.update_db_update_date_and_role();


-- prod.crm_dealer_integration_partner definition

-- Drop table

-- DROP TABLE prod.crm_dealer_integration_partner;

CREATE TABLE prod.crm_dealer_integration_partner (
	id serial4 NOT NULL,
	crm_dealer_id varchar(40) NULL,
	integration_partner_id int4 NOT NULL,
	dealer_id int4 NOT NULL,
	is_active bool DEFAULT false NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	metadata jsonb NULL,
	is_active_salesai bool DEFAULT false NULL,
	is_active_chatai bool DEFAULT false NULL,
	CONSTRAINT pk_crm_dealer_integration_partner PRIMARY KEY (id),
	CONSTRAINT uq_dealer_id UNIQUE (dealer_id),
	CONSTRAINT fk_crm_dealer_integration_partner FOREIGN KEY (integration_partner_id) REFERENCES prod.crm_integration_partner(id),
	CONSTRAINT fk_crm_dealer_integration_partner_dealer FOREIGN KEY (dealer_id) REFERENCES prod.crm_dealer(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_dealer_integration_partner for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_dealer_integration_partner for each row execute function public.update_db_update_date_and_role();


-- prod.crm_salesperson definition

-- Drop table

-- DROP TABLE prod.crm_salesperson;

CREATE TABLE prod.crm_salesperson (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	crm_salesperson_id varchar(100) NULL,
	first_name varchar(50) NULL,
	last_name varchar(50) NULL,
	email varchar(50) NULL,
	phone varchar(20) NULL,
	position_name varchar(50) NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT pk_crm_salesperson PRIMARY KEY (id),
	CONSTRAINT fk_crm_salesperson_dealer_integration_partner FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.crm_dealer_integration_partner(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_salesperson for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_salesperson for each row execute function public.update_db_update_date_and_role();


-- prod.crm_consumer definition

-- Drop table

-- DROP TABLE prod.crm_consumer;

CREATE TABLE prod.crm_consumer (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	crm_consumer_id varchar(50) NULL,
	first_name varchar(50) NULL,
	last_name varchar(50) NULL,
	middle_name varchar(50) NULL,
	email varchar(55) NULL,
	phone varchar(20) NULL,
	postal_code varchar(20) NULL,
	address varchar(100) NULL,
	country varchar(100) NULL,
	city varchar(100) NULL,
	email_optin_flag bool DEFAULT true NULL,
	sms_optin_flag bool DEFAULT true NULL,
	request_product varchar(50) NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	source_system varchar NULL,
	CONSTRAINT pk_crm_consumer PRIMARY KEY (id),
	CONSTRAINT uq_consumer UNIQUE (crm_consumer_id, dealer_integration_partner_id),
	CONSTRAINT fk_crm_consumer_dealer_integration_partner FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.crm_dealer_integration_partner(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_consumer for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_consumer for each row execute function public.update_db_update_date_and_role();


-- prod.crm_lead definition

-- Drop table

-- DROP TABLE prod.crm_lead;

CREATE TABLE prod.crm_lead (
	id serial4 NOT NULL,
	consumer_id int4 NOT NULL,
	crm_lead_id varchar(50) NULL,
	lead_ts timestamptz NULL,
	status varchar(50) NULL,
	substatus varchar(50) NULL,
	"comment" varchar(10000) NULL,
	origin_channel varchar(50) NULL,
	source_channel varchar(100) NULL,
	request_product varchar(50) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	source_detail varchar(100) NULL,
	source_system varchar NULL,
	CONSTRAINT pk_crm_lead PRIMARY KEY (id),
	CONSTRAINT uq_lead UNIQUE (consumer_id, crm_lead_id),
	CONSTRAINT fk_crm_consumer FOREIGN KEY (consumer_id) REFERENCES prod.crm_consumer(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_lead for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_lead for each row execute function public.update_db_update_date_and_role();


-- prod.crm_lead_salesperson definition

-- Drop table

-- DROP TABLE prod.crm_lead_salesperson;

CREATE TABLE prod.crm_lead_salesperson (
	id serial4 NOT NULL,
	lead_id int4 NOT NULL,
	salesperson_id int4 NOT NULL,
	is_primary bool NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
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
    prod.crm_lead_salesperson for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_lead_salesperson for each row execute function public.update_db_update_date_and_role();


-- prod.crm_vehicle definition

-- Drop table

-- DROP TABLE prod.crm_vehicle;

CREATE TABLE prod.crm_vehicle (
	id serial4 NOT NULL,
	lead_id int4 NOT NULL,
	vin varchar(20) NULL,
	crm_vehicle_id varchar(50) NULL,
	stock_num varchar(50) NULL,
	oem_name varchar(80) NULL,
	"type" varchar(100) NULL,
	vehicle_class varchar(80) NULL,
	mileage int4 NULL,
	make varchar(80) NULL,
	model varchar(100) NULL,
	manufactured_year int4 NULL,
	body_style varchar(100) NULL,
	transmission varchar(80) NULL,
	interior_color varchar(80) NULL,
	exterior_color varchar(80) NULL,
	trim varchar(100) NULL,
	price numeric(10, 2) NULL,
	status varchar(50) NULL,
	"condition" varchar(20) NULL,
	odometer_units varchar(20) NULL,
	vehicle_comments varchar(5000) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	trade_in_model varchar(80) NULL,
	trade_in_make varchar(80) NULL,
	trade_in_vin varchar(20) NULL,
	trade_in_year int4 NULL,
	CONSTRAINT pk_crm_vehicle PRIMARY KEY (id),
	CONSTRAINT fk_crm_vehicle_lead FOREIGN KEY (lead_id) REFERENCES prod.crm_lead(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_vehicle for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_vehicle for each row execute function public.update_db_update_date_and_role();


-- prod.crm_activity definition

-- Drop table

-- DROP TABLE prod.crm_activity;

CREATE TABLE prod.crm_activity (
	id serial4 NOT NULL,
	lead_id int4 NOT NULL,
	activity_type_id int4 NOT NULL,
	crm_activity_id varchar(50) NULL,
	activity_requested_ts timestamptz NULL,
	request_product varchar(50) NULL,
	metadata jsonb NULL,
	notes varchar(5000) NULL,
	activity_due_ts timestamptz NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	contact_method varchar(20) NULL,
	CONSTRAINT pk_crm_activity PRIMARY KEY (id),
	CONSTRAINT fk_activity_lead FOREIGN KEY (lead_id) REFERENCES prod.crm_lead(id),
	CONSTRAINT fk_activity_type FOREIGN KEY (activity_type_id) REFERENCES prod.crm_activity_type(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    prod.crm_activity for each row execute function public.create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.crm_activity for each row execute function public.update_db_update_date_and_role();