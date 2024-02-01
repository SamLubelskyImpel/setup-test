# Carlabs ETL

This project does ETL for sales history and repair order from the Carlabs databases to the Shared DMS database.

Sales history data come from the Carlabs table `dataImports` and are mapped to the DMS tables `consumer`, `vehicle`, `vehicle_sale` and `service_contracts` according to the different mappings of each data source.

Repair order data come from the Carlabs table `repair_order` and are mapped to the DMS table `service_repair_order`.

These ETL applications rely on state machines that are triggered nightly and keep running until there's no more data to process for the given day.

The file `app/data_pull.py` contains the entrypoints for both machines. The folder `mapping/` contains the mappings for all the tables loaded by this application.

# Getting Started

All the modules used by this application are listed in `app/requirements.txt`.

# Deployment

To deploy this application you can run the script `deploy.sh` like the follwoing:

```
deploy.sh -e {ENVIRONMENT} -r {eu,us}
```

Run `deploy.sh -h` to see detailed instructions.

The application will be deployed according to the Cloudformation template specified in `template.yml`.

# License
