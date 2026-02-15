---
name: logs
description: View Docker container logs for the Bigeye MCP Server
user-invocable: true
---

# Container Logs

View real-time logs from the Bigeye MCP Server Docker container. This is essential for debugging issues and monitoring application behavior.

## Usage

When invoked, this skill will:
1. Execute the logs command from the project script
2. Show streaming logs from the container
3. Follow logs in real-time until interrupted

## Log Information

Logs typically contain:
- MCP protocol messages and tool invocations
- API request/response information
- Error messages and stack traces

## Implementation

Execute the project's container management script:
```bash
./bigeye-mcp.sh logs
```

The logs command follows logs in real-time. Use Ctrl+C to stop following.
