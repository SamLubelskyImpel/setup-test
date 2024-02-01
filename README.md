# universal_integrations
Re-usable integrations for sourcing data from third-parties (e.g. DMS)

### Using this repo
Each integration should have it's own subfolder with it's own separate deployment script and stack.
This is to avoid dependencies between integrations.

### Deployment
Deployment must be done per subfolder. You must deploy each subfolder that you make changes to
Deploy local env: ./deploy.sh
Deploy stage env: ./deploy.sh -e test
Deploy prod env: ./deploy.sh -e prod

### crm-api
API interactions with the CRM database

### crm-integrations
Stacks per 3rd party client CRM integration

### dms-api
API interactions with the DMS database

### dms-integrations
Stacks per 3rd party client DMS integration

### dms-reporting
Monitoring and reporting for the unified DMS layer

### inventory-integrations
Stacks per 3rd party client inventory integration

### univ-suite
EC2 instance for large file uploads necessary for client integrations

### migrations
Database migrations
