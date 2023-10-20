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
