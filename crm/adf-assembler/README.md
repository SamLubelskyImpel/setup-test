# ADF Assembler
The ADF Assembler is a crucial component of the ADF Layer, responsible for constructing and transmitting the ADF payload to the CRM via email, SFTP, and API. Upon receiving event triggers such as Lead Creation and Appointment Request (triggered by receiving a 'lead_id' or 'activity_id,' respectively), the ADF Payload Assembler constructs ADF payloads tailored to the specific requirements of the target CRM system. This component also handles the formatting and validation of data before transmission. If an error occurs during the assembly of the ADF, it will be logged in CloudWatch, and we can manually retry it. Both events generate ADFs with identical structures, differing only in the comments section, which includes either lead-specific comments or appointment details.

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
https://impel.atlassian.net/wiki/spaces/ENG/pages/3882713093/ADF+Layer+for+Streamlined+CRM+Integrations
