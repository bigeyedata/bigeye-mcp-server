# Bigeye MCP Server

This MCP (Model Context Protocol) server connects to the Bigeye Datawatch API and provides a framework for creating resources, tools, and prompts for interacting with Bigeye's data quality monitoring platform.

## Key Features

### 🔐 Dynamic Authentication
The server now supports dynamic authentication through chat - no configuration files required! Simply provide your API key when prompted, and credentials are securely saved for future sessions.

## Features

### Available Tools

#### Authentication
- **`authenticate_bigeye`** - Authenticate with Bigeye using an API key
- **`use_saved_credentials`** - Use previously saved credentials
- **`switch_workspace`** - Switch to a different workspace in the current instance
- **`list_workspaces`** - List all available workspaces
- **`forget_credentials`** - Remove saved credentials

#### Issue Management
- **`check_health`** - Check the health status of the Bigeye API
- **`get_issues`** - Fetch issues with filtering by status, schema names, and pagination support
- **`merge_issues`** - Merge multiple issues into a single incident (create new or merge into existing)
- **`unmerge_issues`** - Unmerge issues from incidents they have been merged into
- **`get_issue_resolution_steps`** - Get AI-generated resolution steps for issues and incidents
- **`update_issue`** - Update issue status, priority, closing labels, and add timeline messages

#### Lineage Analysis & Root Cause Investigation
- **`get_lineage_node`** - Get details for a specific lineage node (verify existence and properties)
- **`get_lineage_graph`** - Get complete lineage graph for a data entity (upstream/downstream/bidirectional)
- **`analyze_upstream_root_causes`** - Trace upstream lineage to identify root causes of data quality issues
- **`analyze_downstream_impact`** - Analyze downstream impact and affected systems for data quality issues
- **`get_lineage_node_issues`** - Get all data quality issues affecting a specific lineage node
- **`trace_issue_lineage_path`** - Complete lineage trace from root cause to downstream impact for an issue

### Available Resources

- **`bigeye://auth/status`** - Current authentication status and saved credentials
- **`bigeye://health`** - Real-time API health status
- **`bigeye://config`** - Current server configuration and connection details
- **`bigeye://issues`** - All issues from the configured workspace (unfiltered)

### Available Prompts

- **`authentication_flow`** - Guide for authenticating with Bigeye
- **`check_connection_info`** - Guide for verifying API connection setup
- **`merge_issues_example`** - Examples and patterns for merging issues into incidents
- **`lineage_analysis_examples`** - Comprehensive examples for lineage analysis, root cause investigation, and impact assessment

### Key Capabilities

#### Issue & Incident Management
- **Issue Management**: Create, update, close, and merge data quality issues
- **Incident Management**: Merge related issues into incidents for coordinated resolution, and unmerge them when needed
- **Status Tracking**: Update issue statuses with proper validation (requires closing labels when closing issues)
- **Resolution Workflows**: Access AI-generated resolution steps and track progress
- **Flexible Filtering**: Search issues by status, schema, with pagination support
- **Timeline Management**: Add messages and updates to issue timelines
- **Incident Separation**: Unmerge issues from incidents either by specific issue IDs or by incident ID

#### Data Lineage & Root Cause Analysis
- **Lineage Graph Traversal**: Navigate upstream and downstream data dependencies with configurable depth
- **Root Cause Identification**: Automatically identify upstream sources of data quality issues
- **Impact Assessment**: Analyze downstream systems, BI tools, and applications affected by data issues
- **Severity Scoring**: Automatically assess impact severity based on affected systems and integration entities
- **Remediation Planning**: Generate actionable remediation plans based on lineage analysis
- **Progressive Analysis**: Support for quick checks followed by detailed investigation workflows
- **Integration Entity Detection**: Identify affected dashboards, reports, and business applications
- **Issue-to-Lineage Mapping**: Complete tracing from data quality issues through the entire lineage path

## Getting Started

### Authentication Workflow

This MCP server uses dynamic authentication, making it easy to get started:

1. **First Time Use**: When you first interact with the server, it will prompt you for your Bigeye API key
2. **Workspace Selection**: After authentication, you'll see a list of available workspaces to choose from
3. **Secure Storage**: Your credentials are encrypted and stored locally for future sessions
4. **Multi-Instance Support**: You can authenticate with multiple Bigeye instances (demo, app, etc.)
5. **Easy Switching**: Switch between workspaces and instances without re-entering credentials

Example first-time interaction:
```
User: "Show me data quality issues"
Claude: "I need to authenticate with Bigeye first. Please provide your API key."
User: "My API key is bge_xxxxx"
Claude: "Great! I found these workspaces. Which one would you like to use?"
```

## Local Installation

### Prerequisites

1. Install Claude Desktop -- https://claude.ai/download
2. Install uv
```commandline
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Run the MCP server locally
```bash

cd python/bigeye_mcp_server
uv venv
source .venv/bin/activate

# Install dependencies
uv add "mcp[cli]" httpx

# create a local config file
cp config.json.tmpl config.json

# edit the config file to have the right target, API key, and workspace
vi config.json

# next, we have to add the MCP server to Claude Desktop
vi ~/Library/Application\ Support/Claude/claude_desktop_config.json

# if the file is empty, you can paste in the following and replace the templated parts
{
  "mcpServers": {
    "bigeye": {
      "command": "<which uv>",
      "args": [
        "--directory",
        "<path_to_sdp/python>/bigeye_mcp_server",
        "run",
        "server.py"
      ]
    }
  }
}

# if the file is not empty, you can simply add the contents of the above to the existing file
```

## Docker Installation

### Prerequisites

1. Docker and Docker Compose installed on your system
2. Claude Desktop installed -- https://claude.ai/download

### Quick Start with Dynamic Authentication 🔐

The easiest way to get started is with dynamic authentication - no configuration files needed!

1. Clone the repository and navigate to the MCP server directory:
```bash
cd python/bigeye_mcp_server
```

2. Build the Docker image:
```bash
docker build -t bigeye-mcp-server:latest .
```

3. Add to Claude Desktop configuration:
```bash
vi ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

Add this configuration:
```json
{
  "mcpServers": {
    "bigeye": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "-v", "${HOME}/.bigeye-mcp:/home/mcp/.bigeye-mcp",
        "--env", "BIGEYE_DEBUG=${BIGEYE_DEBUG:-false}",
        "bigeye-mcp-server:latest",
        "python",
        "server.py"
      ],
      "env": {
        "BIGEYE_DEBUG": "false"
      }
    }
  }
}
```

4. Restart Claude Desktop and authenticate in chat:
```
User: "I need to analyze data quality issues in Bigeye"
Claude: "I'll help you analyze data quality issues. First, I need to authenticate with your Bigeye instance. Could you provide your Bigeye API key?"
User: "My API key is bge_1234567890abcdef"
Claude: [Authenticates and shows available workspaces]
```

### Multi-Environment Setup

For connecting to different Bigeye instances (demo.bigeye.com and app.bigeye.com):

1. Create environment-specific configurations:
```bash
cp .env.example .env.demo
cp .env.example .env.app
# Edit each file with appropriate credentials
```

2. Use environment-specific docker-compose files:
```bash
# For demo environment
docker-compose -f docker-compose.yml -f docker-compose.demo.yml --env-file .env.demo up

# For app environment  
docker-compose -f docker-compose.yml -f docker-compose.app.yml --env-file .env.app up
```

See `configs/multi-environment-setup.md` for detailed multi-environment configuration.

### Legacy Configuration Options

#### Using Environment Variables

If you prefer using environment variables instead of dynamic authentication:

The Docker setup uses environment variables defined in the `.env` file:
- `BIGEYE_API_KEY` - Your Bigeye API key (required)
- `BIGEYE_BASE_URL` - Bigeye API URL (defaults to https://app.bigeye.com)
- `BIGEYE_WORKSPACE_ID` - Your Bigeye workspace ID (required)
- `BIGEYE_DEBUG` - Enable debug logging (true/false, defaults to false)

#### Using config.json File

Alternatively, you can mount a `config.json` file:

1. Create your config file:
```bash
cp config.json.tmpl config.json
# Edit config.json with your settings
```

2. Uncomment the volume mount in `docker-compose.yml`:
```yaml
volumes:
  - ./config.json:/app/config.json:ro
```

### Integrating with Claude Desktop via Docker

To use the Dockerized MCP server with Claude Desktop:

1. Edit your Claude Desktop configuration:
```bash
vi ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

2. Add the Docker-based MCP server configuration:
```json
{
  "mcpServers": {
    "bigeye-docker": {
      "command": "docker",
      "args": [
        "run",
        "--rm",
        "-i",
        "--env-file", "<path_to_your_env_file>/.env",
        "bigeye-mcp-server:latest"
      ]
    }
  }
}
```

### Development with Docker

For development, you can mount the source code as a volume:

1. Uncomment the development volume in `docker-compose.yml`:
```yaml
volumes:
  - .:/app:ro
```

2. Rebuild and restart the container when making changes:
```bash
docker-compose down
docker-compose build
docker-compose up -d
```

### Testing the Docker Setup

Run the test client to verify the MCP server is working:
```bash
# Start the test client
docker-compose --profile test run test-client

# In another terminal, check server health
docker-compose exec bigeye-mcp python -c "import asyncio; from bigeye_api import BigeyeAPIClient; client = BigeyeAPIClient(); print(asyncio.run(client.check_health()))"
```

### Docker Build and Deployment Scripts

The `scripts/` directory contains helper scripts for building, testing, and deploying the Docker image:

#### Building the Image

```bash
# Build with automatic versioning (uses git branch and commit)
./scripts/build.sh

# Build with specific version
./scripts/build.sh v1.0.0

# Build and push to registry
DOCKER_REGISTRY=ghcr.io/myorg ./scripts/build.sh v1.0.0 --push
```

The build script:
- Automatically generates version tags from git information
- Adds build metadata as Docker labels
- Supports multi-tagging (latest + version)
- Can push directly to a registry with `--push`

#### Testing the Image

```bash
# Run comprehensive tests on the Docker image
./scripts/test.sh

# Test a specific image version
./scripts/test.sh v1.0.0
```

The test script verifies:
- Container startup and basic functionality
- Environment variable loading
- Python imports and dependencies
- STDIO communication compatibility
- Configuration module operation
- Non-root user permissions
- Docker Compose validity

#### Running Locally

```bash
# Run with default .env file
./scripts/run-local.sh

# Run in interactive mode (bash shell)
./scripts/run-local.sh --interactive

# Run with specific config file
./scripts/run-local.sh --config my-config.json

# Run with specific env file and debug mode
./scripts/run-local.sh --env prod.env --debug
```

The run-local script provides:
- Easy local testing without docker-compose
- Interactive mode for debugging
- Config file mounting support
- Environment file selection
- Debug mode toggle

#### Pushing to Registry

```bash
# Push to registry (uses DOCKER_REGISTRY env var)
./scripts/push.sh v1.0.0

# Push to specific registry
./scripts/push.sh v1.0.0 ghcr.io/myorg

# Push latest tag
./scripts/push.sh latest docker.io/mycompany
```

The push script:
- Tags images for the target registry
- Handles authentication checks
- Pushes both version and latest tags
- Provides pull command examples

## IDE Integration

The `configs/` directory contains example configurations for integrating the Dockerized Bigeye MCP Server with various IDEs. See [configs/setup-instructions.md](configs/setup-instructions.md) for detailed setup guides.

### Quick Setup

#### Claude Desktop
```bash
# Copy example config (macOS)
cp configs/claude_desktop_config.json ~/Library/Application\ Support/Claude/claude_desktop_config.json

# Edit to add your credentials
vi ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

#### Cursor
- Add configuration from `configs/cursor_settings.json` to Cursor settings
- Supports environment variables and .env files
- Includes development mode with source mounting

#### VS Code
```bash
# Copy VS Code configuration to your workspace
cp -r configs/.vscode /path/to/your/workspace/

# Configuration includes:
# - MCP server settings
# - Debug launch configurations  
# - Build and test tasks
```

### Configuration Options

All IDE configurations support:
- **Environment Variables**: Pass API credentials via system environment
- **Env File**: Use `.env` file with `--env-file` Docker option
- **Docker Compose**: Integration with docker-compose.yml
- **Development Mode**: Mount source code for live development
- **Multiple Profiles**: Different configs for dev/staging/production

### Security Notes
- Store API keys in environment variables or `.env` files (never in config files)
- Add `.env` to `.gitignore` to prevent accidental commits
- Use read-only volume mounts for production configurations

## Development

It is strongly recommended to use Claude Code for MCP server 
development. A [comprehensive guide can be found here](https://modelcontextprotocol.io/introduction).

Note that Claude Code does not have access to the internet, so we have
to provide it with MCP documentation before doing development. The documentation
which it needs can be found in these two places:

https://modelcontextprotocol.io/llms-full.txt

https://raw.githubusercontent.com/modelcontextprotocol/python-sdk/refs/heads/main/README.md

You can simply copy and paste the contents of the two above links into
Claude Code when you start a dev session.
