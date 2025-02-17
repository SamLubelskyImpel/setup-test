-- prod.appt_integration_partner definition

-- Drop table

-- DROP TABLE prod.appt_integration_partner;

CREATE TABLE prod.appt_integration_partner (
	id serial4 NOT NULL,
	impel_integration_partner_name varchar(40) NOT NULL,
	integration_date timestamptz NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_integration_partner_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_integration_partner for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_integration_partner for each row execute function update_db_update_date_and_role();


-- prod.appt_dealer definition

-- Drop table

-- DROP TABLE prod.appt_dealer;

CREATE TABLE prod.appt_dealer (
	id serial4 NOT NULL,
	dealer_name varchar(80) NOT NULL,
	dealer_location_name varchar(80) NULL,
	country varchar(40) NULL,
	state varchar(20) NULL,
	city varchar(40) NULL,
	zip_code varchar(10) NULL,
	timezone varchar(20) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	sfdc_account_id varchar(40) NOT NULL,
    CONSTRAINT appt_sfdc_account_id UNIQUE (sfdc_account_id),
	CONSTRAINT appt_dealer_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_dealer for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_dealer for each row execute function update_db_update_date_and_role();


-- prod.appt_product definition

-- Drop table

-- DROP TABLE prod.appt_product;

CREATE TABLE prod.appt_product (
	id serial4 NOT NULL,
	product_name varchar(80) NOT NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_product_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_product for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_product for each row execute function update_db_update_date_and_role();


-- prod.appt_dealer_integration_partner definition

-- Drop table

-- DROP TABLE prod.appt_dealer_integration_partner;

CREATE TABLE prod.appt_dealer_integration_partner (
	id serial4 NOT NULL,
	integration_dealer_id varchar(50) NULL,
	integration_partner_id int4 NOT NULL,
	dealer_id int4 NOT NULL,
	product_id int4 NOT NULL,
	is_active bool NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_dealer_integration_partner_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_dealer_integration_partner for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_dealer_integration_partner for each row execute function update_db_update_date_and_role();


-- prod.appt_dealer_integration_partner foreign keys

ALTER TABLE prod.appt_dealer_integration_partner ADD CONSTRAINT appt_dealer_integration_partner_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES prod.appt_dealer(id);
ALTER TABLE prod.appt_dealer_integration_partner ADD CONSTRAINT appt_dealer_integration_partner_integration_partner_id_fkey FOREIGN KEY (integration_partner_id) REFERENCES prod.appt_integration_partner(id);
ALTER TABLE prod.appt_dealer_integration_partner ADD CONSTRAINT appt_dealer_integration_partner_product_id_fkey FOREIGN KEY (product_id) REFERENCES prod.appt_product(id);


-- prod.appt_service_type definition

-- Drop table
-- DROP TABLE prod.appt_service_type;

create table prod.appt_service_type (
	id serial4 not null,
	service_type varchar(255) not null,
	description varchar(255) not null,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_service_type_pkey PRIMARY KEY (id)
)

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_service_type for each row execute function create_db_creation_date_and_role_and_update();

create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_service_type for each row execute function update_db_update_date_and_role();


-- prod.appt_op_code definition

-- Drop table

-- DROP TABLE prod.appt_op_code;

CREATE TABLE prod.appt_op_code (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	op_code varchar(255) NOT NULL,
	op_code_description varchar(255) NULL,
	service_type_id int4 null,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_op_code_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_op_code for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_op_code for each row execute function update_db_update_date_and_role();


-- prod.appt_op_code foreign keys

ALTER TABLE prod.appt_op_code ADD CONSTRAINT appt_op_code_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.appt_dealer_integration_partner(id);
ALTER TABLE prod.appt_op_code ADD CONSTRAINT appt_op_code_service_type_id_fkey FOREIGN KEY (service_type_id) REFERENCES prod.appt_service_type(id);


-- prod.appt_consumer definition

-- Drop table

-- DROP TABLE prod.appt_consumer;

CREATE TABLE prod.appt_consumer (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
	integration_consumer_id varchar(50) NULL,
	product_consumer_id varchar(50) NULL,
	first_name varchar(50) NULL,
	last_name varchar(50) NULL,
	email_address varchar(50) NULL,
	phone_number varchar(15) NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_consumer_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_consumer for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_consumer for each row execute function update_db_update_date_and_role();


-- prod.appt_consumer foreign keys

ALTER TABLE prod.appt_consumer ADD CONSTRAINT appt_consumer_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.appt_dealer_integration_partner(id);


-- prod.appt_vehicle definition

-- Drop table

-- DROP TABLE prod.appt_vehicle;

CREATE TABLE prod.appt_vehicle (
	id serial4 NOT NULL,
	consumer_id int4 NOT NULL,
	vin varchar(20) NULL,
	integration_vehicle_id varchar(50) NULL,
	vehicle_class varchar(40) NULL,
	mileage int4 NULL,
	make varchar(80) NULL,
	model varchar(80) NULL,
	manufactured_year int4 NULL,
	body_style varchar(50) NULL,
	transmission varchar(50) NULL,
	interior_color varchar(50) NULL,
	exterior_color varchar(50) NULL,
	trim varchar(20) NULL,
	"condition" varchar(5) NULL,
	odometer_units varchar(20) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_vehicle_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_vehicle for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_vehicle for each row execute function update_db_update_date_and_role();


-- prod.appt_vehicle foreign keys

ALTER TABLE prod.appt_vehicle ADD CONSTRAINT appt_vehicle_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES prod.appt_consumer(id);


-- prod.appt_appointment definition

-- Drop table

-- DROP TABLE prod.appt_appointment;

CREATE TABLE prod.appt_appointment (
	id serial4 NOT NULL,
	consumer_id int4 NOT NULL,
	vehicle_id int4 NOT NULL,
	integration_appointment_id varchar(50) NULL,
	op_code_id int4 NOT NULL,
	timeslot_ts timestamptz NULL,
	timeslot_duration int4 NULL,
	created_date_ts timestamptz NULL,
	status varchar(50) NULL,
	"comment" varchar(5000) NULL,
	metadata jsonb NULL,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_appointment_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.appt_appointment for each row execute function create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.appt_appointment for each row execute function update_db_update_date_and_role();


-- prod.appt_appointment foreign keys

ALTER TABLE prod.appt_appointment ADD CONSTRAINT appt_appointment_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES prod.appt_consumer(id);
ALTER TABLE prod.appt_appointment ADD CONSTRAINT appt_appointment_op_code_id_fkey foreign key (op_code_id) references prod.appt_op_code(id);
ALTER TABLE prod.appt_appointment ADD CONSTRAINT appt_appointment_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES prod.appt_vehicle(id);
