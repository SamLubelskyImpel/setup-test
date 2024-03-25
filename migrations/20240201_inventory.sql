-- Create a trigger function that sets the db_creation_date and db_update_role fields
CREATE OR REPLACE FUNCTION create_db_creation_date_and_role_and_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.db_creation_date = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    NEW.db_update_date = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    NEW.db_update_role = current_user;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger function that sets the db_update_date and db_update_role fields
CREATE OR REPLACE FUNCTION update_db_update_date_and_role()
RETURNS TRIGGER AS $$
BEGIN
    NEW.db_update_date = CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
    NEW.db_update_role = current_user;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- inv_integration_partner Table
CREATE TABLE stage.inv_integration_partner (
    id SERIAL PRIMARY KEY NOT NULL,
    impel_integration_partner_id VARCHAR(40) NOT NULL,
    "type" VARCHAR(20) NULL,
    db_creation_date timestamptz,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    CONSTRAINT unique_inv_integration_partner_impel_id UNIQUE (impel_integration_partner_id)
);

-- Create a trigger on inv_integration_partner table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_integration_partner'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_integration_partner
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_integration_partner table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_integration_partner'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_integration_partner 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_dealer Table
CREATE TABLE stage.inv_dealer (
    id SERIAL PRIMARY KEY NOT NULL,
    impel_dealer_id VARCHAR(100) NOT NULL,
    sfdc_account_id varchar(40) NOT NULL,
    location_name VARCHAR(80) NULL,
    "state" VARCHAR(20) NULL,
    city VARCHAR(40) NULL,
    zip_code VARCHAR(20) NULL,
    db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    db_update_date timestamptz NULL,
    db_update_role varchar(255) NULL,
    full_name VARCHAR(255) NULL,
    merch_dealer_id VARCHAR(255) NULL,
    salesai_dealer_id VARCHAR(255) NULL,
    merch_is_active BOOLEAN NULL DEFAULT false,
    salesai_is_active BOOLEAN NULL DEFAULT false,
    CONSTRAINT unique_inv_impel_id UNIQUE (impel_dealer_id)
);

-- Create a trigger on inv_dealer table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_dealer'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_dealer
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_dealer table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_dealer'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_dealer 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_dealer_integration_partner Table
CREATE TABLE stage.inv_dealer_integration_partner (
    id SERIAL PRIMARY KEY NOT NULL,
    integration_partner_id INTEGER REFERENCES stage.inv_integration_partner(id) NOT NULL,
    dealer_id INTEGER REFERENCES stage.inv_dealer(id) NOT NULL,
    provider_dealer_id VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    CONSTRAINT inv_dealer_integration_partner_un UNIQUE (dealer_id, integration_partner_id, provider_dealer_id)
);

-- Create a trigger on inv_dealer_integration_partner table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_dealer_integration_partner'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_dealer_integration_partner
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_dealer_integration_partner table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_dealer_integration_partner'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_dealer_integration_partner 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_vehicle Table
CREATE TABLE stage.inv_vehicle (
    id SERIAL PRIMARY KEY NOT NULL,
    vin varchar NULL,
    oem_name VARCHAR(80) NULL,
    "type" VARCHAR(255) NULL,
    vehicle_class VARCHAR(255) NULL,
    mileage VARCHAR(255) NULL,
    make VARCHAR(80) NULL,
    model VARCHAR(80) NULL,
    "year" int4 NULL,
    db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    dealer_integration_partner_id INTEGER REFERENCES stage.inv_dealer_integration_partner(id) NOT NULL,
    new_or_used VARCHAR(1) NULL,
    metadata jsonb NULL,
    stock_num VARCHAR NULL
);

-- Create a trigger on inv_vehicle table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_vehicle'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_vehicle
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_vehicle table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_vehicle'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_vehicle 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_inventory Table
CREATE TABLE stage.inv_inventory (
    id SERIAL PRIMARY KEY NOT NULL,
    vehicle_id INTEGER REFERENCES stage.inv_vehicle(id) NOT NULL,
    db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    list_price FLOAT NULL,
    cost_price FLOAT NULL,
    fuel_type VARCHAR(255) NULL,
    exterior_color VARCHAR(255) NULL,
    interior_color VARCHAR(255) NULL,
    doors INTEGER NULL,
    seats INTEGER NULL,
    transmission VARCHAR(255) NULL,
    photo_url VARCHAR(255) NULL,
    comments VARCHAR(255) NULL,
    drive_train VARCHAR(255) NULL,
    cylinders INTEGER NULL,
    body_style VARCHAR(255) NULL,
    series VARCHAR(255) NULL,
    on_lot BOOLEAN NULL,
    inventory_status VARCHAR(255) NULL,
    vin VARCHAR(255) NULL,
    dealer_integration_partner_id INTEGER REFERENCES stage.inv_dealer_integration_partner(id) NOT NULL,
    interior_material VARCHAR(255) NULL,
    source_data_drive_train VARCHAR(255) NULL,
    source_data_interior_material_description VARCHAR(255) NULL,
    source_data_transmission VARCHAR(255) NULL,
    source_data_transmission_speed VARCHAR(255) NULL,
    transmission_speed VARCHAR(255) NULL,
    build_data VARCHAR(255) NULL,
    region VARCHAR(255) NULL,
    highway_mpg INTEGER NULL,
    city_mpg INTEGER NULL,
    vdp VARCHAR(255) NULL,
    trim VARCHAR(255) NULL,
    special_price FLOAT NULL,
    engine VARCHAR(255) NULL,
    engine_displacement VARCHAR(255) NULL,
    factory_certified BOOLEAN NULL,
    metadata jsonb NULL
);

-- Create a trigger on inv_inventory table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_inventory'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_inventory
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_inventory table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_inventory'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_inventory 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_equipment Table
CREATE TABLE stage.inv_equipment (
    id SERIAL PRIMARY KEY NOT NULL,
    db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    equipment_description VARCHAR(255) NULL,
    is_optional BOOLEAN NULL
);

-- Create a trigger on inv_equipment table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_equipment'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_equipment
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_equipment table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_equipment'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_equipment 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_option Table
CREATE TABLE stage.inv_option (
    id SERIAL PRIMARY KEY NOT NULL,
    db_creation_date timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
    option_description VARCHAR(255) NULL,
    is_priority BOOLEAN NULL
);

-- Create a trigger on inv_option table that fires on INSERT
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_create_db_creation_date_and_role_and_update' 
      AND event_object_table = 'stage.inv_option'
  ) THEN
    CREATE TRIGGER tr_create_db_creation_date_and_role_and_update
    BEFORE INSERT ON stage.inv_option
    FOR EACH ROW
    EXECUTE FUNCTION create_db_creation_date_and_role_and_update();
  END IF;
END $$;

-- Create a trigger on inv_option table that fires on UPDATE
DO $$ 
BEGIN 
  IF NOT EXISTS (
    SELECT 1 
    FROM information_schema.triggers 
    WHERE trigger_name = 'tr_update_db_update_date_and_role' 
      AND event_object_table = 'stage.inv_option'
  ) THEN
    CREATE TRIGGER tr_update_db_update_date_and_role
    BEFORE UPDATE ON stage.inv_option 
    FOR EACH ROW
    EXECUTE FUNCTION update_db_update_date_and_role();
  END IF;
END $$;

-- inv_option_inventory Table
CREATE TABLE stage.inv_option_inventory (
    id SERIAL PRIMARY KEY NOT NULL,
    inv_option_id INTEGER REFERENCES stage.inv_option(id) NOT NULL,
    inv_inventory_id INTEGER REFERENCES stage.inv_inventory(id) NOT NULL,
    CONSTRAINT unique_inv_option_inventory UNIQUE (inv_option_id, inv_inventory_id)
);

-- inv_equipment_inventory Table
CREATE TABLE stage.inv_equipment_inventory (
    id SERIAL PRIMARY KEY NOT NULL,
    inv_equipment_id INTEGER REFERENCES stage.inv_equipment(id) NOT NULL,
    inv_inventory_id INTEGER REFERENCES stage.inv_inventory(id) NOT NULL,
    CONSTRAINT unique_inv_equipment_inventory UNIQUE (inv_equipment_id, inv_inventory_id)
);
