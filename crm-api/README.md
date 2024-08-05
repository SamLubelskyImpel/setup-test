# CRM API

The CRM API is the interface for the centralized CRM persistance layer which manages interactions between CRM partners and Impel products.

## Deployment and teardown

### How to deploy

The easiest way to deploy is using the `deploy.sh` script found in the project's root. This script has three modes:

* **`./deploy.sh`** - by itself, the script will deploy to test using an auto-generated environment name.
The name is composed of your IAM username and currently-checked-out branch.
* **`./deploy.sh -e stage`** will deploy to the common staging environment. Keep in mind that multiple branches may be merged
into the staging environment so coordinate with other developers.
* **`./deploy.sh -e prod`** will deploy to production.

## Folder Structure and ORM Layer

The CRM API organizes its endpoints into specific folders depending on their database relation.
These folders include `activity`, `consumer`, `lead`, etc.

For database interactions, the CRM API defines various ORM classes. In order to support the folder structure, these classes have been made accessible through the `data_layer` lambda layer.

## Testing

To execute the local testing suite, run the following in this root folder:

```
BASE_URL={CRM API URL for your environment} pytest
```

## Requirements

- CRM API and related resources deployed to a testenv
- `unified-test` profile configured in your AWS local config

## Use cases

The test suite currently covers the following use cases:

- Create lead endpoint sends event to WBNS
- Create activity endpoint sends event to WBNS

## License

This software is proprietary, Augmented Reality Concepts Inc., d.b.a SpinCar.

## Appendix

### Confluence Documentation
https://impel.atlassian.net/wiki/spaces/ENG/pages/3723165715/CRM+API
