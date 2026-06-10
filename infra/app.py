#!/usr/bin/env python3
import aws_cdk as cdk

from mcp_server_stack import McpServerStack

app = cdk.App()

McpServerStack(
    app,
    "BigeyeMcpServer",
    env=cdk.Environment(account="021451147547", region="us-west-2"),
)

app.synth()
