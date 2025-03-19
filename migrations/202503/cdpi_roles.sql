------------------CDPI----------------------
-- Passwords removed for security reasons, pg_dump and pg_restore are used to migrate the database data
-- Reader
create user reader with password '';
grant connect on database postgres to reader;
GRANT USAGE ON SCHEMA test TO reader;
grant select on all tables in schema test to reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA test GRANT SELECT ON TABLES TO reader;

-- Developer
create user developer with password '';
grant connect on database postgres to developer;
GRANT USAGE ON SCHEMA test TO developer;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA test TO developer;

grant usage on sequence test.cdpi_consumer_id_seq to developer;
grant usage on sequence test.cdpi_consumer_profile_id_seq to developer;
grant usage on sequence test.cdpi_dealer_id_seq to developer;
grant usage on sequence test.cdpi_dealer_integration_partner_id_seq to developer;
grant usage on sequence test.cdpi_integration_partner_id_seq to developer;
grant usage on sequence test.cdpi_product_id_seq to developer;
grant usage on sequence test.cdpi_audit_log_id_seq to developer;
grant usage on sequence test.cdpi_audit_dsr_id_seq to developer;

-- Application
create user application with password '';
grant connect on database postgres to application;
GRANT USAGE ON SCHEMA test TO application;
GRANT SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA test TO application;

grant usage on sequence test.cdpi_consumer_id_seq to application;
grant usage on sequence test.cdpi_consumer_profile_id_seq to application;
grant usage on sequence test.cdpi_dealer_id_seq to application;
grant usage on sequence test.cdpi_dealer_integration_partner_id_seq to application;
grant usage on sequence test.cdpi_integration_partner_id_seq to application;
grant usage on sequence test.cdpi_product_id_seq to application;
grant usage on sequence test.cdpi_audit_log_id_seq to application;
grant usage on sequence test.cdpi_audit_dsr_id_seq to application;