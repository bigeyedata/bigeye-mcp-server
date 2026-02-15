---
name: status
description: Check Docker container status for the Bigeye MCP Server
user-invocable: true
---

# Container Status

Check the current status of the Bigeye MCP Server Docker container. This provides a quick overview of whether the service is running, stopped, or not created.

## Usage

When invoked, this skill will:
1. Execute the status command from the project script
2. Display the current state of the container
3. Show container details if running

## Output

The status check shows:
- Container name and current status (running/stopped/not created)
- Image name and uptime information

## Implementation

Execute the project's container management script:
```bash
./bigeye-mcp.sh status
```
