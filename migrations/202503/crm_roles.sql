------------------CRM----------------------
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

-- Application
create user application with password '';
grant connect on database dms to application;
GRANT USAGE ON SCHEMA test TO application;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA test TO application;

GRANT USAGE ON SCHEMA stage TO application;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA stage TO application;

grant usage on sequence stage.crm_activity_id_seq to application;
grant usage on sequence stage.crm_activity_type_id_seq to application;
grant usage on sequence stage.crm_dealer_id_seq to application;
grant usage on sequence stage.crm_dealer_integration_partner_id_seq to application;
grant usage on sequence stage.crm_integration_partner_id_seq to application;
grant usage on sequence stage.crm_lead_id_seq to application;
grant usage on sequence stage.crm_consumer_id_seq to application;
grant usage on sequence stage.crm_lead_salesperson_id_seq to application;
grant usage on sequence stage.crm_salesperson_id_seq to application;
grant usage on sequence stage.crm_vehicle_id_seq to application;

grant usage on sequence test.crm_activity_id_seq to application;
grant usage on sequence test.crm_activity_type_id_seq to application;
grant usage on sequence test.crm_dealer_id_seq to application;
grant usage on sequence test.crm_dealer_integration_partner_id_seq to application;
grant usage on sequence test.crm_integration_partner_id_seq to application;
grant usage on sequence test.crm_lead_id_seq to application;
grant usage on sequence test.crm_consumer_id_seq to application;
grant usage on sequence test.crm_lead_salesperson_id_seq to application;
grant usage on sequence test.crm_salesperson_id_seq to application;
grant usage on sequence test.crm_vehicle_id_seq to application;

-- Developer
create user developer with password '';
grant connect on database dms to reader;
GRANT USAGE ON SCHEMA test TO developer;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA test TO developer;

GRANT USAGE ON SCHEMA stage TO developer;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA stage TO developer;

grant usage on sequence stage.crm_activity_id_seq to developer;
grant usage on sequence stage.crm_activity_type_id_seq to developer;
grant usage on sequence stage.crm_dealer_id_seq to developer;
grant usage on sequence stage.crm_dealer_integration_partner_id_seq to developer;
grant usage on sequence stage.crm_integration_partner_id_seq to developer;
grant usage on sequence stage.crm_lead_id_seq to developer;
grant usage on sequence stage.crm_consumer_id_seq to developer;
grant usage on sequence stage.crm_lead_salesperson_id_seq to developer;
grant usage on sequence stage.crm_salesperson_id_seq to developer;
grant usage on sequence stage.crm_vehicle_id_seq to developer;

grant usage on sequence test.crm_activity_id_seq to developer;
grant usage on sequence test.crm_activity_type_id_seq to developer;
grant usage on sequence test.crm_dealer_id_seq to developer;
grant usage on sequence test.crm_dealer_integration_partner_id_seq to developer;
grant usage on sequence test.crm_integration_partner_id_seq to developer;
grant usage on sequence test.crm_lead_id_seq to developer;
grant usage on sequence test.crm_consumer_id_seq to developer;
grant usage on sequence test.crm_lead_salesperson_id_seq to developer;
grant usage on sequence test.crm_salesperson_id_seq to developer;
grant usage on sequence test.crm_vehicle_id_seq to developer;