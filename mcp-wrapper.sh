#!/bin/bash

# MCP wrapper script for Claude Desktop
# This script ensures the container is running, then connects via docker exec.
# Use this as the "command" in claude_desktop_config.json.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_NAME="bigeye-mcp-server"

# Check if the container is running
if ! docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "Starting Bigeye MCP Server container..." >&2
    docker compose -p bigeye-mcp -f "${SCRIPT_DIR}/docker-compose.yml" up -d bigeye-mcp
    sleep 2
fi

# Connect to the MCP server
docker exec -i "$CONTAINER_NAME" python server.py
