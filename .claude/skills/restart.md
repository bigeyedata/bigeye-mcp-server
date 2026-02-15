---
name: restart
description: Restart the Docker container for the Bigeye MCP Server
user-invocable: true
---

# Container Restart

Restart the Bigeye MCP Server Docker container. This is useful when you need to refresh the service or recover from container issues without rebuilding the image.

## Usage

When invoked, this skill will:
1. Restart the container via docker compose
2. Report success

## Use Cases

- Apply configuration changes that require restart
- Recover from container crashes or hangs
- Reset application state

## Implementation

Execute the project's container management script:
```bash
./bigeye-mcp.sh restart
```
