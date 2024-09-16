# universal_integrations
Re-usable integrations for sourcing data from third-parties (e.g. DMS, CRM, INV, APPT)

### Using this repo
Each integration should have it's own subfolder with it's own separate deployment script and stack.
This is to avoid dependencies between integrations.

### Deployment
Deployment must be done per subfolder. You must deploy each subfolder that you make changes to
Deploy local env: ./deploy.sh
Deploy stage env: ./deploy.sh -e test
Deploy prod env: ./deploy.sh -e prod


### appointment
Stacks related to the Appointment domain

### crm
Stacks related to the CRM domain

### dms
Stacks related to the DMS domain

### inventory
Stacks related to the INV domain

### univ-suite
EC2 instance for large file uploads necessary for client integrations

### migrations
Database migrations

### DDL
DDL files for shared integration layers

### email-service
Shared email distribution service

### notification-service
Shared notification service (INS)

### data-reprocessing
Scripts to assist developers when reprocessing data in the AWS environment