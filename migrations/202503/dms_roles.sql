------------------DMS----------------------
-- Passwords removed for security reasons, pg_dump and pg_restore are used to migrate the database data
-- Reader
create user reader with password '';
grant connect on database dms to reader;
GRANT USAGE ON SCHEMA test TO reader;
grant select on all tables in schema test to reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES TO reader;

GRANT USAGE ON SCHEMA stage TO reader;
grant select on all tables in schema stage to reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT SELECT ON TABLES TO reader;

-- Fivetran
create user fivetran with password '';
grant connect on database dms to fivetran;
GRANT USAGE ON SCHEMA "stage" TO fivetran;
GRANT SELECT ON ALL TABLES IN SCHEMA "stage" TO fivetran;
ALTER DEFAULT PRIVILEGES IN SCHEMA "stage" GRANT SELECT ON TABLES TO fivetran;

-- Application
create user application with password '';
grant connect on database dms to application;
GRANT USAGE ON SCHEMA stage TO application;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA stage TO application;

GRANT USAGE ON SCHEMA test TO application;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA test TO application;

grant usage on sequence stage.consumer_id_seq to application;
grant usage on sequence stage.dealer_id_seq to application;
grant usage on sequence stage.dealer_group_id_seq to application;
grant usage on sequence stage.dealer_integration_partner_id_seq to application;
grant usage on sequence stage.integration_partner_id_seq to application;
grant usage on sequence stage.inventory_id_seq to application;
grant usage on sequence stage.op_code_id_seq to application;
grant usage on sequence stage.op_code_repair_order_id_seq to application;
grant usage on sequence stage.service_contracts_id_seq to application;
grant usage on sequence stage.service_repair_order_id_seq to application;
grant usage on sequence stage.sfdc_account_id_seq to application;
grant usage on sequence stage.vehicle_id_seq to application;
grant usage on sequence stage.vehicle_sale_id_seq to application;
grant usage on sequence stage.appointment_id_seq to application;
grant usage on sequence stage.op_code_appointment_id_seq to application;

grant usage on sequence test.consumer_id_seq to application;
grant usage on sequence test.dealer_id_seq to application;
grant usage on sequence test.dealer_group_id_seq to application;
grant usage on sequence test.dealer_integration_partner_id_seq to application;
grant usage on sequence test.integration_partner_id_seq to application;
grant usage on sequence test.inventory_id_seq to application;
grant usage on sequence test.op_code_id_seq to application;
grant usage on sequence test.op_code_repair_order_id_seq to application;
grant usage on sequence test.service_contracts_id_seq to application;
grant usage on sequence test.service_repair_order_id_seq to application;
grant usage on sequence test.sfdc_account_id_seq to application;
grant usage on sequence test.vehicle_id_seq to application;
grant usage on sequence test.vehicle_sale_id_seq to application;

-- Developer
create user developer with password '';
grant connect on database dms to developer;
GRANT USAGE ON SCHEMA stage TO developer;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA stage TO developer;

GRANT USAGE ON SCHEMA test TO developer;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA test TO developer;

grant usage on sequence stage.consumer_id_seq to developer;
grant usage on sequence stage.dealer_id_seq to developer;
grant usage on sequence stage.dealer_group_id_seq to developer;
grant usage on sequence stage.dealer_integration_partner_id_seq to developer;
grant usage on sequence stage.integration_partner_id_seq to developer;
grant usage on sequence stage.inventory_id_seq to developer;
grant usage on sequence stage.op_code_id_seq to developer;
grant usage on sequence stage.op_code_repair_order_id_seq to developer;
grant usage on sequence stage.service_contracts_id_seq to developer;
grant usage on sequence stage.service_repair_order_id_seq to developer;
grant usage on sequence stage.sfdc_account_id_seq to developer;
grant usage on sequence stage.vehicle_id_seq to developer;
grant usage on sequence stage.vehicle_sale_id_seq to developer;
grant usage on sequence stage.appointment_id_seq to developer;
grant usage on sequence stage.op_code_appointment_id_seq to developer;

grant usage on sequence test.consumer_id_seq to developer;
grant usage on sequence test.dealer_id_seq to developer;
grant usage on sequence test.dealer_group_id_seq to developer;
grant usage on sequence test.dealer_integration_partner_id_seq to developer;
grant usage on sequence test.integration_partner_id_seq to developer;
grant usage on sequence test.inventory_id_seq to developer;
grant usage on sequence test.op_code_id_seq to developer;
grant usage on sequence test.op_code_repair_order_id_seq to developer;
grant usage on sequence test.service_contracts_id_seq to developer;
grant usage on sequence test.service_repair_order_id_seq to developer;
grant usage on sequence test.sfdc_account_id_seq to developer;
grant usage on sequence test.vehicle_id_seq to developer;
grant usage on sequence test.vehicle_sale_id_seq to developer;