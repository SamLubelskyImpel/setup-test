-- Create Service Type Table
create table test.appt_service_type (
	id serial4 not null,
	service_type varchar(255) not null,
	description varchar(255) not null,
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_service_type_pkey PRIMARY KEY (id)
)

create trigger tr_create_db_creation_date_and_role_and_update before
insert
    on
    test.appt_service_type for each row execute function create_db_creation_date_and_role_and_update();

create trigger tr_update_db_update_date_and_role before
update
    on
    test.appt_service_type for each row execute function update_db_update_date_and_role();

-- Update Op Code
alter table test.appt_op_code add column service_type_id int4 null;
ALTER TABLE test.appt_op_code ADD CONSTRAINT appt_op_code_service_type_id_fkey FOREIGN KEY (service_type_id) REFERENCES test.appt_service_type(id);

-- Update Appointment
alter table test.appt_appointment add column op_code_id int4 null;
alter table test.appt_appointment add CONSTRAINT appt_appointment_op_code_id_fkey foreign key (op_code_id) references test.appt_op_code(id);

-- Initialize Service Type
insert into test.appt_service_type (service_type, description) values ('GENERAL_SERVICE', 'General Vehicle Maintenance');

-- Update Op Code with Service Type
update test.appt_op_code set service_type_id = (select id from test.appt_service_type where service_type = 'GENERAL_SERVICE');

-- Update Appointment with Op Code instead of Op Code Appointment ID
update test.appt_appointment set op_code_id = (select op_code_id from test.appt_op_code_appointment where id = test.appt_appointment.op_code_appointment_id);

-- Add SFDC Account ID to Dealer
alter table test.appt_dealer add column sfdc_account_id varchar(40) null;

-- -- Drop Op Code Appointment
-- alter table test.appt_appointment alter column op_code_appointment_id set null;
-- drop table test.appt_op_code_appointment;

-- -- Drop Op Code Product
-- drop table test.appt_op_code_product;
