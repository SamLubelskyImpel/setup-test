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
	CONSTRAINT pk_cdpi_integration_partner PRIMARY KEY (id),
    CONSTRAINT uq_impel_integration_partner_name UNIQUE (impel_integration_partner_name)
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
	CONSTRAINT pk_cdpi_product PRIMARY KEY (id),
    CONSTRAINT uq_product_name UNIQUE (product_name)
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
    consumer_id int4 NOT NULL,
    integration_partner_id int4 NOT NULL,
	cdp_master_consumer_id varchar(100) NOT NULL,
	cdp_dealer_consumer_id varchar(100) NOT NULL,
	score_update_date timestamptz NULL,
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
    CONSTRAINT uq_cdpi_consumer_integration_partner UNIQUE (consumer_id, integration_partner_id),
    CONSTRAINT fk_cdpi_consumer FOREIGN KEY (consumer_id) REFERENCES test.cdpi_consumer(id),
    CONSTRAINT fk_cdpi_integration_partner FOREIGN KEY (integration_partner_id) REFERENCES test.cdpi_integration_partner(id)
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


-- test.cdpi_consumer definition

-- Drop table

-- DROP TABLE test.cdpi_consumer;

CREATE TABLE test.cdpi_consumer (
	id serial4 NOT NULL,
    product_id int4 NOT NULL,
    dealer_id int4 NOT NULL,
	source_consumer_id varchar(100) NOT NULL,
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
    record_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    db_update_date timestamptz NULL,
    db_creation_date timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT pk_cdpi_consumer PRIMARY KEY (id),
	CONSTRAINT uq_product_consumer UNIQUE (dealer_id, product_id, source_consumer_id),
	CONSTRAINT fk_cdpi_dealer FOREIGN KEY (dealer_id) REFERENCES test.cdpi_dealer(id),
    CONSTRAINT fk_cdpi_product FOREIGN KEY (product_id) REFERENCES test.cdpi_product(id)
);

-- Table Triggers

create trigger tr_create_db_creation_date_and_role before
insert
    on
    test.cdpi_consumer for each row execute function create_db_creation_date_and_role();
create trigger tr_update_db_update_date_and_role before
update
    on
    test.cdpi_consumer for each row execute function update_db_update_date_and_role();

CREATE TABLE test.cdpi_audit_log (
    id SERIAL PRIMARY KEY,
    table_name varchar NOT NULL,
    operation varchar NOT NULL,  -- e.g., 'UPDATE', 'INSERT'
    row_id varchar NOT NULL,     -- ID of the modified row
    changed_columns varchar[],   -- List of changed columns
    old_values JSONB,         -- Values before the update (for updates)
    new_values JSONB,         -- New values after the update
    db_update_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    db_update_role varchar(255)
);


CREATE OR REPLACE FUNCTION log_audit_update() RETURNS TRIGGER AS $$
DECLARE
    changed_cols varchar[] := ARRAY[]::varchar[];  -- Initialize an empty array for changed columns
    old_vals JSONB := '{}'::JSONB;  -- JSONB to hold old values
    new_vals JSONB := '{}'::JSONB;  -- JSONB to hold new values
    update_role varchar;
    col varchar;
    col_value_old text;
    col_value_new text;
    excluded_cols varchar[] := ARRAY['db_update_date', 'db_update_role', 'db_creation_date'];
BEGIN
    -- Loop through each column in the updated row and check if the individual column value has changed
    FOR col IN SELECT column_name FROM information_schema.columns WHERE table_name = TG_TABLE_NAME loop
	    -- Skip columns that should not be tracked
        IF col = ANY (excluded_cols) THEN
            CONTINUE;  -- Skip this column and move to the next one
        END IF;
        -- Get the old and new values of the current column
        EXECUTE format('SELECT $1.%I::text', col) INTO col_value_old USING OLD;
        EXECUTE format('SELECT $1.%I::text', col) INTO col_value_new USING NEW;

        -- Compare the old and new values for each column
        IF col_value_old IS DISTINCT FROM col_value_new THEN
            -- If different, add the column to the changed columns array
            changed_cols := array_append(changed_cols, col);
            -- Add old and new values to JSONB objects
            old_vals := old_vals || jsonb_build_object(col, to_jsonb(col_value_old));
            new_vals := new_vals || jsonb_build_object(col, to_jsonb(col_value_new));
        END IF;
    END LOOP;

    -- If no columns were changed, skip the insert
    IF array_length(changed_cols, 1) IS NULL THEN
        RETURN NEW;  -- No changes, so return without inserting
    END IF;

    -- Insert the audit log entry with dynamically captured update_role
    INSERT INTO test.cdpi_audit_log (table_name, operation, row_id, changed_columns, old_values, new_values, db_update_role)
    VALUES (TG_TABLE_NAME, TG_OP, NEW.id, changed_cols, old_vals, new_vals, current_user);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER audit_update_trigger
AFTER UPDATE ON test.cdpi_consumer
FOR EACH ROW EXECUTE FUNCTION log_audit_update();