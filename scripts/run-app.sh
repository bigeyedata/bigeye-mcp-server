#!/bin/bash
# Run Bigeye MCP Server for App (Production) Environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Starting Bigeye MCP Server for App (Production) Environment..."
echo "Base URL: https://app.bigeye.com"

# Load environment variables from .env.app
if [ -f "$PROJECT_ROOT/.env.app" ]; then
    export $(cat "$PROJECT_ROOT/.env.app" | grep -v '^#' | xargs)
fi

# Run docker-compose with app configuration
docker-compose \
    -f "$PROJECT_ROOT/docker-compose.yml" \
    -f "$PROJECT_ROOT/docker-compose.app.yml" \
    --env-file "$PROJECT_ROOT/.env.app" \
    up bigeye-mcp-app