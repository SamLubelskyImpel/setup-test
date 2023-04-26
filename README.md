# universal_integrations
Re-usable integrations for sourcing data from third-parties (e.g. DMS)

### Running the application

Make sure virtual environment is installed and activated, you can create a virtual env with:

```
make venv
```

The development environment is configured to use docker workflow. To build the docker image run:

```
make build
```

Start the application with
```
make run
```

Stop the application with:

```
make stop
```

To test that the application is running as expected, the health_check of the app returns the status of the app, you can use the below command or access the url at http://localhost:8090/health_check

```
make ping
```

### Deploy the application


The deploy.sh scripts takes requires some arguments. To get more info on the deploy script run:


```
bash deploy.sh -h
```

Deployment example:

```
bash deploy.sh -p unified-data-test-dev -e test
```
