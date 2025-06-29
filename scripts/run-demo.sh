#!/bin/bash
# Run Bigeye MCP Server for Demo Environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Starting Bigeye MCP Server for Demo Environment..."
echo "Base URL: https://demo.bigeye.com"

# Load environment variables from .env.demo
if [ -f "$PROJECT_ROOT/.env.demo" ]; then
    export $(cat "$PROJECT_ROOT/.env.demo" | grep -v '^#' | xargs)
fi

# Run docker-compose with demo configuration
docker-compose \
    -f "$PROJECT_ROOT/docker-compose.yml" \
    -f "$PROJECT_ROOT/docker-compose.demo.yml" \
    --env-file "$PROJECT_ROOT/.env.demo" \
    up bigeye-mcp-demo