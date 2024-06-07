-- prod.inv_dealer definition

-- Drop table

-- DROP TABLE prod.inv_dealer;

CREATE TABLE prod.inv_dealer (
	id serial4 NOT NULL,
	impel_dealer_id varchar(100) NOT NULL,
	sfdc_account_id varchar(40) NOT NULL,
	location_name varchar(80) NULL,
	state varchar(20) NULL,
	city varchar(40) NULL,
	zip_code varchar(20) NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	full_name varchar(255) NULL,
	merch_dealer_id varchar(255) NULL,
	salesai_dealer_id varchar(255) NULL,
	merch_is_active bool DEFAULT false NULL,
	salesai_is_active bool DEFAULT false NULL,
	CONSTRAINT inv_dealer_pkey PRIMARY KEY (id),
	CONSTRAINT unique_inv_impel_id UNIQUE (impel_dealer_id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_dealer for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_dealer for each row execute function public.update_db_update_date_and_role();


-- prod.inv_equipment definition

-- Drop table

-- DROP TABLE prod.inv_equipment;

CREATE TABLE prod.inv_equipment (
	id serial4 NOT NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	equipment_description varchar(255) NULL,
	is_optional bool NULL,
	CONSTRAINT inv_equipment_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_equipment for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_equipment for each row execute function public.update_db_update_date_and_role();


-- prod.inv_integration_partner definition

-- Drop table

-- DROP TABLE prod.inv_integration_partner;

CREATE TABLE prod.inv_integration_partner (
	id serial4 NOT NULL,
	impel_integration_partner_id varchar(40) NOT NULL,
	"type" varchar(20) NULL,
	db_creation_date timestamptz NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT inv_integration_partner_pkey PRIMARY KEY (id),
	CONSTRAINT unique_inv_integration_partner_impel_id UNIQUE (impel_integration_partner_id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_integration_partner for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_integration_partner for each row execute function public.update_db_update_date_and_role();


-- prod.inv_option definition

-- Drop table

-- DROP TABLE prod.inv_option;

CREATE TABLE prod.inv_option (
	id serial4 NOT NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	option_description varchar(255) NULL,
	is_priority bool NULL,
	CONSTRAINT inv_option_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_option for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_option for each row execute function public.update_db_update_date_and_role();


-- prod.inv_dealer_integration_partner definition

-- Drop table

-- DROP TABLE prod.inv_dealer_integration_partner;

CREATE TABLE prod.inv_dealer_integration_partner (
	id serial4 NOT NULL,
	integration_partner_id int4 NOT NULL,
	dealer_id int4 NOT NULL,
	provider_dealer_id varchar(255) NOT NULL,
	is_active bool DEFAULT true NOT NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT inv_dealer_integration_partner_pkey PRIMARY KEY (id),
	CONSTRAINT inv_dealer_integration_partner_un UNIQUE (dealer_id, integration_partner_id, provider_dealer_id),
	CONSTRAINT inv_dealer_integration_partner_dealer_id_fkey FOREIGN KEY (dealer_id) REFERENCES prod.inv_dealer(id),
	CONSTRAINT inv_dealer_integration_partner_integration_partner_id_fkey FOREIGN KEY (integration_partner_id) REFERENCES prod.inv_integration_partner(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_dealer_integration_partner for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_dealer_integration_partner for each row execute function public.update_db_update_date_and_role();


-- prod.inv_vehicle definition

-- Drop table

-- DROP TABLE prod.inv_vehicle;

CREATE TABLE prod.inv_vehicle (
	id serial4 NOT NULL,
	vin varchar NULL,
	oem_name varchar(80) NULL,
	"type" varchar(255) NULL,
	vehicle_class varchar(255) NULL,
	mileage varchar(255) NULL,
	make varchar(80) NULL,
	model varchar(80) NULL,
	"year" int4 NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	dealer_integration_partner_id int4 NOT NULL,
	new_or_used varchar(1) NULL,
	metadata jsonb NULL,
	stock_num varchar NULL,
	CONSTRAINT inv_vehicle_pkey PRIMARY KEY (id),
	CONSTRAINT inv_vehicle_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.inv_dealer_integration_partner(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_vehicle for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_vehicle for each row execute function public.update_db_update_date_and_role();


-- prod.inv_inventory definition

-- Drop table

-- DROP TABLE prod.inv_inventory;

CREATE TABLE prod.inv_inventory (
	id serial4 NOT NULL,
	vehicle_id int4 NOT NULL,
	db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	list_price float8 NULL,
	cost_price float8 NULL,
	fuel_type varchar(255) NULL,
	exterior_color varchar(255) NULL,
	interior_color varchar(255) NULL,
	doors int4 NULL,
	seats int4 NULL,
	transmission varchar(255) NULL,
	photo_url varchar(255) NULL,
	"comments" varchar(255) NULL,
	drive_train varchar(255) NULL,
	cylinders int4 NULL,
	body_style varchar(255) NULL,
	series varchar(255) NULL,
	on_lot bool NULL,
	inventory_status varchar(255) NULL,
	vin varchar(255) NULL,
	dealer_integration_partner_id int4 NOT NULL,
	interior_material varchar(255) NULL,
	source_data_drive_train varchar(255) NULL,
	source_data_interior_material_description varchar(255) NULL,
	source_data_transmission varchar(255) NULL,
	source_data_transmission_speed varchar(255) NULL,
	transmission_speed varchar(255) NULL,
	build_data varchar(255) NULL,
	region varchar(255) NULL,
	highway_mpg int4 NULL,
	city_mpg int4 NULL,
	vdp varchar(255) NULL,
	trim varchar(255) NULL,
	special_price float8 NULL,
	engine varchar(255) NULL,
	engine_displacement varchar(255) NULL,
	factory_certified bool NULL,
	metadata jsonb NULL,
	received_datetime timestamptz NULL,
	CONSTRAINT inv_inventory_pkey PRIMARY KEY (id),
	CONSTRAINT inv_inventory_dealer_integration_partner_id_fkey FOREIGN KEY (dealer_integration_partner_id) REFERENCES prod.inv_dealer_integration_partner(id),
	CONSTRAINT inv_inventory_vehicle_id_fkey FOREIGN KEY (vehicle_id) REFERENCES prod.inv_vehicle(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    prod.inv_inventory for each row execute function public.create_db_creation_date_and_role_and_update();
create trigger tr_update_db_update_date_and_role before
update
    on
    prod.inv_inventory for each row execute function public.update_db_update_date_and_role();


-- prod.inv_option_inventory definition

-- Drop table

-- DROP TABLE prod.inv_option_inventory;

CREATE TABLE prod.inv_option_inventory (
	id serial4 NOT NULL,
	inv_option_id int4 NOT NULL,
	inv_inventory_id int4 NOT NULL,
	CONSTRAINT inv_option_inventory_pkey PRIMARY KEY (id),
	CONSTRAINT unique_inv_option_inventory UNIQUE (inv_option_id, inv_inventory_id),
	CONSTRAINT inv_option_inventory_inv_inventory_id_fkey FOREIGN KEY (inv_inventory_id) REFERENCES prod.inv_inventory(id),
	CONSTRAINT inv_option_inventory_inv_option_id_fkey FOREIGN KEY (inv_option_id) REFERENCES prod.inv_option(id)
);


-- prod.inv_equipment_inventory definition

-- Drop table

-- DROP TABLE prod.inv_equipment_inventory;

CREATE TABLE prod.inv_equipment_inventory (
	id serial4 NOT NULL,
	inv_equipment_id int4 NOT NULL,
	inv_inventory_id int4 NOT NULL,
	CONSTRAINT inv_equipment_inventory_pkey PRIMARY KEY (id),
	CONSTRAINT unique_inv_equipment_inventory UNIQUE (inv_equipment_id, inv_inventory_id),
	CONSTRAINT inv_equipment_inventory_inv_equipment_id_fkey FOREIGN KEY (inv_equipment_id) REFERENCES prod.inv_equipment(id),
	CONSTRAINT inv_equipment_inventory_inv_inventory_id_fkey FOREIGN KEY (inv_inventory_id) REFERENCES prod.inv_inventory(id)
);