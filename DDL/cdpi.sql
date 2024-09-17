------------- Schema functions
-- DROP FUNCTION test.create_db_creation_date_and_role();

CREATE OR REPLACE FUNCTION test.create_db_creation_date_and_role()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.db_creation_date = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    NEW.db_update_role = current_user;
    RETURN NEW;
END;
$function$
;

-- DROP FUNCTION test.update_db_update_date_and_role();

CREATE OR REPLACE FUNCTION test.update_db_update_date_and_role()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.db_update_date = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    NEW.db_update_role = current_user;
    RETURN NEW;
END;
$function$
;


------------- Table Schemas + Triggers

-- test.cdpi_integration_partner definition

-- Drop table

-- DROP TABLE test.cdpi_integration_partner;

CREATE TABLE test.cdpi_integration_partner (
	id serial4 NOT NULL,
	impel_integration_partner_name varchar(40) NOT NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_integration_partner PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_integration_partner for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_integration_partner for each row execute function update_db_update_date_and_role();


-- test.cdpi_dealer definition

-- Drop table

-- DROP TABLE test.cdpi_dealer;

CREATE TABLE test.cdpi_dealer (
	id serial4 NOT NULL,
    dealer_name varchar(80) NOT NULL,
	salesai_dealer_id varchar(80) NULL,
    serviceai_dealer_id varchar(80) NULL,
	sfdc_account_id varchar(40) NOT NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_dealer PRIMARY KEY (id),
	CONSTRAINT uq_sfdc_account_id UNIQUE (sfdc_account_id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_dealer for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_dealer for each row execute function update_db_update_date_and_role();


-- test.cdpi_product definition

-- Drop table

-- DROP TABLE test.cdpi_product;

CREATE TABLE test.cdpi_product (
	id serial4 NOT NULL,
	product_name varchar(40) NOT NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_product PRIMARY KEY (id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_product for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_product for each row execute function update_db_update_date_and_role();


-- test.cdpi_dealer_integration_partner definition

-- Drop table

-- DROP TABLE test.cdpi_dealer_integration_partner;

CREATE TABLE test.cdpi_dealer_integration_partner (
	id serial4 NOT NULL,
    integration_partner_id int4 NOT NULL,
	dealer_id int4 NOT NULL,
	cdp_dealer_id varchar(40) NULL,
	is_active bool DEFAULT false NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_dealer_integration_partner PRIMARY KEY (id),
	CONSTRAINT uq_integration_dealer UNIQUE (integration_partner_id, cdp_dealer_id),
	CONSTRAINT fk_cdpi_integration_partner FOREIGN KEY (integration_partner_id) REFERENCES test.cdpi_integration_partner(id),
	CONSTRAINT fk_cdpi_dealer FOREIGN KEY (dealer_id) REFERENCES test.cdpi_dealer(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_dealer_integration_partner for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_dealer_integration_partner for each row execute function update_db_update_date_and_role();


-- test.cdpi_consumer_profile definition

-- Drop table

-- DROP TABLE test.cdpi_consumer_profile;

CREATE TABLE test.cdpi_consumer_profile (
	id serial4 NOT NULL,
	cdp_master_consumer_id varchar(100) NOT NULL,
	cdp_dealer_consumer_id varchar(100) NOT NULL,
	data_load_date timestamptz NULL,
	master_pscore_sales varchar(40) NULL,
    master_pscore_service varchar(40) NULL,
    dealer_pscore_sales varchar(40) NULL,
    dealer_pscore_service varchar(40) NULL,
    prev_master_pscore_sales varchar(40) NULL,
    prev_master_pscore_service varchar(40) NULL,
    prev_dealer_pscore_sales varchar(40) NULL,
    prev_dealer_pscore_service varchar(40) NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_consumer_profile PRIMARY KEY (id),
    CONSTRAINT uq_cdp_consumer_id UNIQUE (cdp_master_consumer_id, cdp_dealer_consumer_id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_consumer_profile for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_consumer_profile for each row execute function update_db_update_date_and_role();


-- test.cdpi_consumer_identity definition

-- Drop table

-- DROP TABLE test.cdpi_consumer_identity;

CREATE TABLE test.cdpi_consumer_identity (
	id serial4 NOT NULL,
	dealer_integration_partner_id int4 NOT NULL,
    product_id int4 NOT NULL,
    consumer_profile_id int4 NULL,
	source_consumer_id varchar(100) NOT NULL,
    data_load_date timestamptz NULL,
	first_name varchar(80) NULL,
	last_name varchar(80) NULL,
	email varchar(100) NULL,
	phone varchar(15) NULL,
	email_optin_flag bool NULL,
    phone_optin_flag bool NULL,
    sms_optin_flag bool NULL,
    address_line_1 varchar(80) NULL,
    address_line_2 varchar(80) NULL,
    city varchar(80) NULL,
    zip varchar(10) NULL,
    zipextra varchar(10) NULL,
    country varchar(80) NULL,
    suite varchar(80) NULL,
    areatype varchar(80) NULL,
    area varchar(80) NULL,
    pobox varchar(80) NULL,
    hh_id varchar(80) NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_consumer_identity PRIMARY KEY (id),
	CONSTRAINT uq_product_consumer_identity UNIQUE (dealer_integration_partner_id, product_id, source_consumer_id),
	CONSTRAINT fk_cdpi_dealer_integration_partner FOREIGN KEY (dealer_integration_partner_id) REFERENCES test.cdpi_dealer_integration_partner(id),
    CONSTRAINT fk_cdpi_product FOREIGN KEY (product_id) REFERENCES test.cdpi_product(id),
    CONSTRAINT fk_cdpi_consumer_profile FOREIGN KEY (consumer_profile_id) REFERENCES test.cdpi_consumer_profile(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_consumer_identity for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_consumer_identity for each row execute function update_db_update_date_and_role();
