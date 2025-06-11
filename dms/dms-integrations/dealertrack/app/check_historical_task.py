import boto3
from os import environ
import logging

ENV = environ.get("ENVIRONMENT")
QUEUE_URL = environ.get("QUEUE_URL")
CLUSTER = f"dealertrack-{ENV}-ECSHistoricalPullCluster"
TASK_DEFINITION = f"dealertrack-{ENV}-HistoricalPullTask"
SUBNETS = (
    [
        "subnet-0d29a385efe83bf1c",
        "subnet-0e88ecdd743701e96",
        "subnet-00291e028e21cb78f",
        "subnet-0b1555d5fa3c8ba8e",
    ]
    if ENV == "prod"
    else [
        "subnet-030d57e39ec0df603",
        "subnet-01044d580678ea63c",
        "subnet-0b29db0aeb6cdabec",
        "subnet-0e28d592f2ca28fb7",
    ]
)
SECURITY_GROUP = environ.get("SECURITY_GROUP")


ecs = boto3.client("ecs")
sqs = boto3.client("sqs")
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    tasks = ecs.list_tasks(
        cluster=CLUSTER,
        family=TASK_DEFINITION,
        desiredStatus="RUNNING",
    )["taskArns"]

    logger.info(f"ECS is running tasks: {tasks}")

    if tasks:
        return

    events_pending = sqs.get_queue_attributes(QueueUrl=QUEUE_URL, AttributeNames=["ApproximateNumberOfMessages"])
    logger.info(f"Historical pull queue: {events_pending}")
    if not int(events_pending.get("Attributes", {}).get("ApproximateNumberOfMessages", 0)):
        return

    task = ecs.run_task(
        cluster=CLUSTER,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": SUBNETS,
                "securityGroups": [SECURITY_GROUP],
                "assignPublicIp": "DISABLED",
            }
        },
        taskDefinition=TASK_DEFINITION
    )

    logger.info(f"Started task: {task}")
