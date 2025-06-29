# Multi-Environment Setup for Bigeye MCP Server

This guide explains how to configure and run the Bigeye MCP Server for multiple Bigeye environments (demo.bigeye.com and app.bigeye.com) using Docker.

## Overview

The multi-environment setup allows you to:
- Connect to different Bigeye instances (demo and production)
- Use different workspace IDs for each environment
- Manage separate API keys for each environment
- Run multiple MCP server instances simultaneously

## Configuration Files

### Environment-Specific Docker Compose Files

- `docker-compose.yml` - Base configuration
- `docker-compose.demo.yml` - Demo environment overrides
- `docker-compose.app.yml` - App (production) environment overrides

### Environment Variable Files

- `.env.demo` - Demo environment variables
- `.env.app` - App environment variables

Copy and configure these files:
```bash
cp .env.example .env.demo
cp .env.example .env.app
```

Edit each file with your environment-specific values:
- `BIGEYE_DEMO_API_KEY` / `BIGEYE_APP_API_KEY`
- `BIGEYE_DEMO_WORKSPACE_ID` / `BIGEYE_APP_WORKSPACE_ID`

## Running the Servers

### Using Helper Scripts

Run the demo environment:
```bash
./scripts/run-demo.sh
```

Run the app environment:
```bash
./scripts/run-app.sh
```

### Using Docker Compose Directly

For demo environment:
```bash
docker-compose -f docker-compose.yml -f docker-compose.demo.yml --env-file .env.demo up bigeye-mcp-demo
```

For app environment:
```bash
docker-compose -f docker-compose.yml -f docker-compose.app.yml --env-file .env.app up bigeye-mcp-app
```

## Claude Desktop Configuration

Add the following to your Claude Desktop configuration file (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

### Option 1: Direct Docker Configuration

```json
{
  "mcpServers": {
    "bigeye-demo": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "--env", "BIGEYE_API_KEY=${BIGEYE_API_KEY}",
        "--env", "BIGEYE_BASE_URL=${BIGEYE_BASE_URL}",
        "--env", "BIGEYE_WORKSPACE_ID=${BIGEYE_WORKSPACE_ID}",
        "--env", "BIGEYE_DEBUG=${BIGEYE_DEBUG:-false}",
        "bigeye-mcp-server:latest",
        "python",
        "server.py"
      ],
      "env": {
        "BIGEYE_API_KEY": "your_demo_api_key_here",
        "BIGEYE_BASE_URL": "https://demo.bigeye.com",
        "BIGEYE_WORKSPACE_ID": "your_demo_workspace_id_here",
        "BIGEYE_DEBUG": "false"
      }
    },
    "bigeye-app": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "--env", "BIGEYE_API_KEY=${BIGEYE_API_KEY}",
        "--env", "BIGEYE_BASE_URL=${BIGEYE_BASE_URL}",
        "--env", "BIGEYE_WORKSPACE_ID=${BIGEYE_WORKSPACE_ID}",
        "--env", "BIGEYE_DEBUG=${BIGEYE_DEBUG:-false}",
        "bigeye-mcp-server:latest",
        "python",
        "server.py"
      ],
      "env": {
        "BIGEYE_API_KEY": "your_app_api_key_here",
        "BIGEYE_BASE_URL": "https://app.bigeye.com",
        "BIGEYE_WORKSPACE_ID": "your_app_workspace_id_here",
        "BIGEYE_DEBUG": "false"
      }
    }
  }
}
```

### Option 2: Docker Compose Configuration

```json
{
  "mcpServers": {
    "bigeye-demo-compose": {
      "command": "docker-compose",
      "args": [
        "-f", "/path/to/bigeye_mcp_server/docker-compose.yml",
        "-f", "/path/to/bigeye_mcp_server/docker-compose.demo.yml",
        "--env-file", "/path/to/bigeye_mcp_server/.env.demo",
        "run",
        "--rm",
        "-T",
        "bigeye-mcp-demo",
        "python",
        "server.py"
      ]
    },
    "bigeye-app-compose": {
      "command": "docker-compose",
      "args": [
        "-f", "/path/to/bigeye_mcp_server/docker-compose.yml",
        "-f", "/path/to/bigeye_mcp_server/docker-compose.app.yml",
        "--env-file", "/path/to/bigeye_mcp_server/.env.app",
        "run",
        "--rm",
        "-T",
        "bigeye-mcp-app",
        "python",
        "server.py"
      ]
    }
  }
}
```

Remember to replace `/path/to/bigeye_mcp_server` with the actual path to your project directory.

## Environment Variables

The following environment variables are used:

- `BIGEYE_API_KEY` - Your Bigeye API key
- `BIGEYE_BASE_URL` - The Bigeye instance URL (https://demo.bigeye.com or https://app.bigeye.com)
- `BIGEYE_WORKSPACE_ID` - Your Bigeye workspace ID
- `BIGEYE_DEBUG` - Enable debug logging (true/false)

## Building the Docker Image

Before running any environment, build the Docker image:

```bash
docker build -t bigeye-mcp-server:latest .
```

Or use the build script:
```bash
./scripts/build.sh
```

## Testing

To test a specific environment:

```bash
# Test demo environment
docker run --rm -it \
  -e BIGEYE_API_KEY="your_demo_key" \
  -e BIGEYE_BASE_URL="https://demo.bigeye.com" \
  -e BIGEYE_WORKSPACE_ID="your_demo_workspace_id" \
  bigeye-mcp-server:latest \
  python server.py

# Test app environment
docker run --rm -it \
  -e BIGEYE_API_KEY="your_app_key" \
  -e BIGEYE_BASE_URL="https://app.bigeye.com" \
  -e BIGEYE_WORKSPACE_ID="your_app_workspace_id" \
  bigeye-mcp-server:latest \
  python server.py
```

## Troubleshooting

1. **Container name conflicts**: If you get an error about container names already in use, stop the existing container:
   ```bash
   docker stop bigeye-mcp-server-demo
   docker stop bigeye-mcp-server-app
   ```

2. **Environment variables not loading**: Ensure your `.env.demo` and `.env.app` files are in the correct location and have the proper format.

3. **Connection issues**: Verify that your API keys and workspace IDs are correct for each environment.

4. **Claude Desktop not recognizing the server**: Restart Claude Desktop after updating the configuration file.