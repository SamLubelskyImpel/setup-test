from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_rds as rds,
    Duration,
    RemovalPolicy
)
from constructs import Construct


class UnifiedData(Stack):

    def __init__(
        self, scope: Construct, construct_id: str, environment: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc.from_lookup(
            self, "TestVPC", vpc_id="vpc-0b28df8980a1905d5"
        )
        security_group = ec2.SecurityGroup.from_security_group_id(
            self, "RDSSecurityGroup", security_group_id="sg-00b911ee260ce5153"
        )
        subnet_group = rds.SubnetGroup.from_subnet_group_name(
            self, "TestDBPrivateSubnetGroup", "db-subnet-group-test-private"
        )

        crm_rds = rds.DatabaseInstance(  # noqa: F841
            self,
            "CRMLayerRDS",
            instance_identifier=f"unified-crm-{environment}",
            vpc=vpc,
            security_groups=[security_group],
            subnet_group=subnet_group,
            multi_az=False,

            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_7
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL
            ),
            database_name="dms",
            port=5432,
            allocated_storage=100,
            credentials=rds.Credentials.from_generated_secret(username="pgroot"),
            iam_authentication=True,
            storage_encrypted=True,
            auto_minor_version_upgrade=True,
            backup_retention=Duration.days(7),
            copy_tags_to_snapshot=True,
            enable_performance_insights=False,
            preferred_backup_window="04:09-04:39",
            preferred_maintenance_window="sun:07:33-sun:08:03",
            publicly_accessible=False,
            removal_policy=RemovalPolicy.RETAIN,
        )

        dms_rds = rds.DatabaseInstance(  # noqa: F841
            self,
            "DMSLayerRDS",
            instance_identifier=f"unified-dms-{environment}",
            vpc=vpc,
            security_groups=[security_group],
            subnet_group=subnet_group,
            multi_az=False,

            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_7
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL
            ),
            database_name="dms",
            port=5432,
            allocated_storage=100,
            credentials=rds.Credentials.from_generated_secret(username="pgroot"),
            iam_authentication=True,
            storage_encrypted=True,
            auto_minor_version_upgrade=True,
            backup_retention=Duration.days(7),
            copy_tags_to_snapshot=True,
            enable_performance_insights=False,
            preferred_backup_window="04:09-04:39",
            preferred_maintenance_window="sun:07:33-sun:08:03",
            publicly_accessible=False,
            removal_policy=RemovalPolicy.RETAIN,
        )

        cdpi_rds = rds.DatabaseInstance(  # noqa: F841
            self,
            "CDPILayerRDS",
            instance_identifier=f"unified-cdpi-{environment}",
            vpc=vpc,
            security_groups=[security_group],
            subnet_group=subnet_group,
            multi_az=False,

            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_7
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL
            ),
            database_name="dms",
            port=5432,
            allocated_storage=100,
            credentials=rds.Credentials.from_generated_secret(username="pgroot"),
            iam_authentication=True,
            storage_encrypted=True,
            auto_minor_version_upgrade=True,
            backup_retention=Duration.days(7),
            copy_tags_to_snapshot=True,
            enable_performance_insights=False,
            preferred_backup_window="04:09-04:39",
            preferred_maintenance_window="sun:07:33-sun:08:03",
            publicly_accessible=False,
            removal_policy=RemovalPolicy.RETAIN,
        )

        shared_rds = rds.DatabaseInstance(  # noqa: F841
            self,
            "SharedLayerRDS",
            instance_identifier=f"unified-shared-{environment}",
            vpc=vpc,
            security_groups=[security_group],
            subnet_group=subnet_group,
            multi_az=False,

            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_7
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T4G, ec2.InstanceSize.SMALL
            ),
            database_name="dms",
            port=5432,
            allocated_storage=100,
            credentials=rds.Credentials.from_generated_secret(username="pgroot"),
            iam_authentication=True,
            storage_encrypted=True,
            auto_minor_version_upgrade=True,
            backup_retention=Duration.days(7),
            copy_tags_to_snapshot=True,
            enable_performance_insights=False,
            preferred_backup_window="04:09-04:39",
            preferred_maintenance_window="sun:07:33-sun:08:03",
            publicly_accessible=False,
            removal_policy=RemovalPolicy.RETAIN,
        )
