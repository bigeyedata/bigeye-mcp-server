from aws_cdk import (
    Stack,
    Duration,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecs_patterns as ecs_patterns,
    aws_secretsmanager as secretsmanager,
    aws_logs as logs,
)
from constructs import Construct

VPC_ID = "vpc-9d63b5e5"


class McpServerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc.from_lookup(self, "Vpc", vpc_id=VPC_ID)

        cluster = ecs.Cluster(
            self,
            "Cluster",
            vpc=vpc,
            enable_fargate_capacity_providers=True,
        )

        creds = secretsmanager.Secret(
            self,
            "SharedBigeyeCreds",
            secret_name="bigeye-mcp-server/shared-credentials",
            description="Shared Bigeye API key + workspace id for the hosted MCP server",
        )

        service = ecs_patterns.ApplicationLoadBalancedFargateService(
            self,
            "Service",
            cluster=cluster,
            cpu=256,
            memory_limit_mib=512,
            desired_count=2,
            public_load_balancer=False,
            min_healthy_percent=100,
            max_healthy_percent=200,
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
            capacity_provider_strategies=[
                ecs.CapacityProviderStrategy(capacity_provider="FARGATE_SPOT", weight=1),
            ],
            task_image_options=ecs_patterns.ApplicationLoadBalancedTaskImageOptions(
                image=ecs.ContainerImage.from_asset("..", file="Dockerfile"),
                container_port=8080,
                environment={
                    "MCP_TRANSPORT": "streamable-http",
                    "MCP_PORT": "8080",
                    "BIGEYE_BASE_URL": "https://app.bigeye.com",
                },
                secrets={
                    "BIGEYE_API_KEY": ecs.Secret.from_secrets_manager(creds, "api_key"),
                    "BIGEYE_WORKSPACE_ID": ecs.Secret.from_secrets_manager(creds, "workspace_id"),
                },
                log_driver=ecs.LogDriver.aws_logs(
                    stream_prefix="bigeye-mcp",
                    log_retention=logs.RetentionDays.ONE_MONTH,
                ),
            ),
            assign_public_ip=True,
            task_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            health_check_grace_period=Duration.seconds(60),
        )

        service.target_group.configure_health_check(
            path="/health",
            healthy_http_codes="200",
            interval=Duration.seconds(30),
        )

        service.load_balancer.set_attribute(
            "idle_timeout.timeout_seconds", "300"
        )
