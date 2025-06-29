# IDE Configuration Setup Instructions

This directory contains example configuration files for integrating the Dockerized Bigeye MCP Server with various IDEs and editors.

## Prerequisites

1. Docker installed and running
2. Bigeye MCP Server Docker image built (`./scripts/build.sh`)
3. Environment variables set or `.env` file created
4. API credentials from Bigeye

## Claude Desktop Setup

### Option 1: Using Environment Variables

1. Copy the appropriate configuration from `claude_desktop_config.json` to your Claude Desktop config:
   ```bash
   # macOS
   cp configs/claude_desktop_config.json ~/Library/Application\ Support/Claude/claude_desktop_config.json
   
   # Windows
   # Copy to %APPDATA%\Claude\claude_desktop_config.json
   
   # Linux
   # Copy to ~/.config/Claude/claude_desktop_config.json
   ```

2. Edit the configuration and replace placeholder values:
   - `your_api_key_here` with your actual Bigeye API key
   - `your_workspace_id_here` with your workspace ID

3. Restart Claude Desktop

### Option 2: Using .env File

1. Use the `bigeye-docker-envfile` configuration from the example
2. Update the path to your `.env` file
3. Ensure your `.env` file contains all required variables

## Cursor Setup

1. Open Cursor settings (Cmd/Ctrl + ,)
2. Search for "MCP" or "Model Context Protocol"
3. Add the configuration from `cursor_settings.json`
4. For environment variables, either:
   - Set them in your system environment
   - Use the `bigeye-envfile` configuration with a `.env` file
5. Restart Cursor

## VS Code Setup

### Installing MCP Extension

1. Install the MCP extension for VS Code (if available)
2. Copy the `.vscode` folder from `configs/` to your workspace root:
   ```bash
   cp -r configs/.vscode /path/to/your/workspace/
   ```

### Configuration Options

The VS Code setup includes:

1. **settings.json** - MCP server configurations
2. **launch.json** - Debug configurations for testing
3. **tasks.json** - Build and deployment tasks

### Using the Configuration

1. Open VS Code in your workspace
2. The MCP servers should be available in the MCP panel
3. Use the provided tasks:
   - `Ctrl/Cmd + Shift + B` - Build Docker image
   - `Ctrl/Cmd + Shift + P` > "Tasks: Run Task" - See all available tasks

## Environment Variables

All configurations support environment variables. Set these in your shell profile:

```bash
# Add to ~/.bashrc, ~/.zshrc, or equivalent
export BIGEYE_API_KEY="your_api_key"
export BIGEYE_API_URL="https://app.bigeye.com"
export BIGEYE_WORKSPACE_ID="your_workspace_id"
export BIGEYE_DEBUG="false"
```

Or create a `.env` file in your project root:

```env
BIGEYE_API_KEY=your_api_key
BIGEYE_API_URL=https://app.bigeye.com
BIGEYE_WORKSPACE_ID=your_workspace_id
BIGEYE_DEBUG=false
```

## Docker Registry Usage

If using a private Docker registry:

1. Set the `DOCKER_REGISTRY` environment variable:
   ```bash
   export DOCKER_REGISTRY="ghcr.io/yourorg"
   ```

2. Build and push the image:
   ```bash
   ./scripts/build.sh --push
   ```

3. Update the configurations to use the registry URL:
   ```json
   "args": [
     "run", "--rm", "-i",
     "--env-file", ".env",
     "ghcr.io/yourorg/bigeye-mcp-server:latest"
   ]
   ```

## Troubleshooting

### Container Not Starting

1. Check Docker is running: `docker info`
2. Verify image exists: `docker images | grep bigeye-mcp`
3. Test manually: `./scripts/test.sh`

### Authentication Issues

1. Verify environment variables are set: `echo $BIGEYE_API_KEY`
2. Check `.env` file permissions: `ls -la .env`
3. Test API connection: `./scripts/run-local.sh --debug`

### IDE Not Connecting

1. Check IDE logs for MCP errors
2. Verify stdio transport is working
3. Try running the container manually to see output
4. Ensure the IDE has permission to run Docker commands

## Security Best Practices

1. **Never commit API keys** - Use environment variables or `.env` files
2. **Add `.env` to `.gitignore`** - Prevent accidental commits
3. **Use read-only mounts** - Mount config files as `:ro`
4. **Rotate API keys regularly** - Update in all configurations
5. **Use secrets management** - Consider Docker secrets for production