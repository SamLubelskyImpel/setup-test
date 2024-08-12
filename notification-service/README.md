# Impel Notification Service

This service aims to be a central notification service to integrate with multiple partners on the shared layer and other Impel products.

## Deployment and teardown

### How to deploy

The easiest way to deploy is using the `deploy.sh` script found in the project's root. This script has three modes:

* **`./deploy.sh`** - by itself, the script will deploy to test using an auto-generated environment name.
The name is composed of your IAM username and currently-checked-out branch.
* **`./deploy.sh -e test`** will deploy to the common testing environment. Keep in mind that multiple branches may be merged into the testing environment so coordinate with other developers.
* **`./deploy.sh -e prod`** will deploy to production.

## Testing

To execute the local testing suite, run the following in this root folder:

```
pytest
```

## Requirements

- INS deployed to `test`
- `unified-test` and `test` profiles configured in your AWS local config

## Use cases

The test suite currently covers the following use cases:

- Event sent to INS triggers the `WebhookWritebackLambda` and reaches the `event-logger` webhook
- Event sent to INS is logged by the monitoring Lambda
