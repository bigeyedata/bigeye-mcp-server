---
name: rebuild
description: Rebuild Docker image and recreate container for the Bigeye MCP Server
user-invocable: true
---

# Container Rebuild

Rebuild the Docker image and recreate the container for the Bigeye MCP Server. This skill is used after code changes to rebuild the image with both tags (`bigeye-mcp-server:latest` and `bigeye-mcp-ephemeral:latest`), remove the old container, and start a fresh one.

## Usage

When invoked, this skill will:
1. Build the Docker image with both required tags
2. Remove the old container
3. Recreate and start the container via docker compose

## Implementation

Execute the project's container management script:
```bash
./bigeye-mcp.sh rebuild
```
