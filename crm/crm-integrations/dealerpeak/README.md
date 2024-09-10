# Dealerpeak CRM Integration

The Dealerpeak CRM integration at Impel is designed to seamlessly handle CRM data ingestion, transformation, and storage. The integration calls Dealerpeak’s APIs on a schedule of every 5 minutes to pull new leads. Leads are filtered by created time. Real time lead updates are request based, they are retrieved from DealerPeak’s APIs upon request to the integration layer. Activities are syndicated back to the CRM through provided APIs. Architecture details and partner documentation can be found on the Confluence page. See the appendix for more information.

## Deployment and teardown

### How to deploy

The easiest way to deploy is using the `deploy.sh` script found in the project's root. This script has three modes:

* **`./deploy.sh`** - by itself, the script will deploy to test using an auto-generated environment name.
The name is composed of your IAM username and currently-checked-out branch.
* **`./deploy.sh -e test`** will deploy to the common testing environment. Keep in mind that multiple branches may be merged into the testing environment so coordinate with other developers.
* **`./deploy.sh -e prod`** will deploy to production.

## License

This software is proprietary, Augmented Reality Concepts Inc., d.b.a SpinCar.

## Appendix

### Confluence Documentation
https://impel.atlassian.net/wiki/spaces/ENG/pages/3733651486/DealerPeak+Integration
