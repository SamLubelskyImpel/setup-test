# CarSales AU CRM Integration

The CarSales AU CRM integration at Impel is designed to seamlessly handle CRM data ingestion, transformation, and storage. CarSales CRM sends leads to an Impel provided webhook, developed based on CarSales specifications. This webhook receives leads from dealers integrated with both CarSales and DealerSocket AU. Leads are routed to the correct integration resource based on the partner assigned to the dealership. Lead updates are not supported for CarSales leads. Additionally, we have logic in place for updating the lead status in CarSales CRM to report first contact. Architecture details and partner documentation can be found on the Confluence page. See the appendix for more information.

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
