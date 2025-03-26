#!/usr/bin/env python3
# cdk deploy -c env=test --profile unified-test
import aws_cdk as cdk
from stacks.unified_data import UnifiedData
import boto3
import git

app = cdk.App()

# Environment
environment = app.node.try_get_context("env") or "test"
config = {
    "test": {"account": "143813444726", "region": "us-east-1"}
}
if environment not in config:
    raise ValueError(f"Invalid environment '{environment}'. Choose from: {list(config.keys())}")

cdk_env = cdk.Environment(
    account=config[environment]["account"],
    region=config[environment]["region"]
)

# Tags
iam_client = boto3.client("iam")
user_name = iam_client.get_user()["User"]["UserName"]

repo = git.Repo(search_parent_directories=True)
commit_id = repo.head.object.hexsha

cdk.Tags.of(app).add("UserLastModified", user_name)
cdk.Tags.of(app).add("Environment", environment)
cdk.Tags.of(app).add("Commit", commit_id)

# Stacks
UnifiedData(
    app, f"UnifiedData-{environment}",
    environment=environment,
    env=cdk_env
)

app.synth()
