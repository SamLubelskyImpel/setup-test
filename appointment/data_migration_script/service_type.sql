-- Create Service Type Table
create table test.appt_service_type (
	id serial4 not null,
	service_type varchar(255) not null
	description varchar(255) not null
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
	CONSTRAINT appt_service_type_pkey PRIMARY KEY (id)
)

INSERT INTO test.appt_service_type (
    service_type varchar(255) not null
	description varchar(255) not null
	db_creation_date timestamptz DEFAULT now() NULL,
	db_update_date timestamptz NULL,
	db_update_role varchar(255) NULL,
)
SELECT
    op_code,
    op_code_description,
    db_creation_date,
    db_update_date,
    db_update_role
FROM test.appt_op_code_product