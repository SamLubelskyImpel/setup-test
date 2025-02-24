# Description

This application integrates with the Dealertrack DMS. Refer to this page for further documentation: https://impel.atlassian.net/wiki/spaces/ENG/pages/4518281226/Dealertrack+Integration

# SentinelOne Agent

The ECS workload which powers the historical pull is protected by a SentinelOne Agent which is deployed as part of the task definition. Refer to this article to understand how it's injected in the task: https://usea1-017.sentinelone.net/docs/en/deploying-the-agent-to-ecs-on-aws-fargate.html#deploying-the-agent-to-ecs-on-aws-fargate

# Deployment

To deploy the regular Cloudformation resources defined at the template, you can use the command `./deploy.sh -e {prod,test,dev}`

The historical pull requires a Docker image to be built and pushed to our private repository on AWS. Once the previous deployment is finished, run: `./deploy_image.sh -e {prod,test,dev} -p {AWS_PROFILE}`.

Any changes on the data pull code will require both deployment scripts to be executed.

Due to the amount of permissions required to push images to the repository, the platform team decided to only give them to the dev admin role. Please be aware of this when trying to deploy the image.

# Testing

To test the historical pull locally, run `QUEUE_URL=https://sqs.us-east-1.amazonaws.com/143813444726/dealertrack-test-HistoricalPullQueue INTEGRATIONS_BUCKET=integrations-us-east-1-test AWS_PROFILE=unified-test python app/historical_pull.py`. Files will be stored on S3 but the code will run locally so you can watch the logs and debug the execution.