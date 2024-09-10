# Momentum CRM Integration

The Momentum CRM integration at Impel is designed to seamlessly handle CRM data ingestion, transformation, and storage. This process is divided into two main parts. The first part of the integration is managed through the CRM Partner API, which enables us to receive leads and lead updates in real time and save them into the S3 bucket. In this repository, we handle the second part, which consists of the transformation and load process. The raw data received from Momentum is transformed and stored in a unified shared CRM database via CRM API. Additionally, we have logic in place for writing activities back to Momentum CRM. Architecture details and partner documentation can be found on the Confluence page. See the appendix for more information.

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
https://impel.atlassian.net/wiki/spaces/ENG/pages/3826188325/Momentum+CRM+Integration
