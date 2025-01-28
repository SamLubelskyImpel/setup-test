# Inventory Partner API

The Inventory Partner API allows Inventory partners to send us data in real time and serves as the first part of the integration process. It receives requests from the partner, authenticates them, and saves the raw data to the S3 bucket (inventory-integrations-{region}-{environment}) in the raw folder. The process of transforming this raw data and saving it to the Impel database is handled separately. Details about authentication, endpoints, requests, and responses can be found in the Swagger documentation. Architecture details and the diagram can be found on the Confluence page. See the appendix for more information.

## Deployment and teardown

### How to deploy

The easiest way to deploy is using the `deploy.sh` script found in the project's root. This script has three modes:

* **`./deploy.sh`** - by itself, the script will deploy to test using an auto-generated environment name.
The name is composed of your IAM username and currently-checked-out branch.
* **`./deploy.sh -e test`** will deploy to the common testing environment. Keep in mind that multiple branches may be merged into the testing environment so coordinate with other developers.
* **`./deploy.sh -e prod`** will deploy to production.

## Folder Structure

The Inventory Partner API organizes its endpoints into specific folders depending on the partner name.

## License

This software is proprietary, Augmented Reality Concepts Inc., d.b.a SpinCar.

## Appendix

### Confluence Documentation
TBD

### Swagger Documentation
TBD