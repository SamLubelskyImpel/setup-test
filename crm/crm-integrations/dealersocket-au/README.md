# DealerSocket AU CRM Integration

The DealerSocket AU CRM integration at Impel is designed to seamlessly handle CRM data ingestion, transformation, and storage. This process is divided into two main parts. The first part of the integration is managed through the CarSales AU New Lead Webhook, which enables us to receive leads associated with DealerSocket dealerships. In this repository, we handle the second part, which consists of CRM lead lookup, transformation and load process. The raw data received from DealerSocket AU is transformed and stored in a unified shared CRM database via CRM API. Additionally, we have logic in place for writing activities back to DealerSocket AU CRM. Architecture details and partner documentation can be found on the Confluence page. See the appendix for more information.

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
TBD
