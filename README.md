# Bigeye MCP Server

An MCP (Model Context Protocol) server that provides tools for interacting with the Bigeye Data Observability platform.

## Features

- Query and manage data quality issues
- Analyze data lineage and dependencies
- Track AI agent data access patterns
- Perform root cause analysis for data quality issues
- Manage incidents and issue resolution

## 🔐 Configuration

**Important**: This server requires credentials to be configured in your Claude Desktop configuration file. There are no fallbacks - if credentials are not provided via environment variables, the server will exit with instructions on how to configure them.

### Claude Desktop Configuration (Required)

The Bigeye MCP server runs as an ephemeral Docker container that spins up only when Claude Desktop needs it. Configure it in your Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "bigeye": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "BIGEYE_API_KEY=your_api_key_here",
        "-e",
        "BIGEYE_API_URL=https://your-instance.bigeye.com",
        "-e",
        "BIGEYE_WORKSPACE_ID=your_workspace_id_here",
        "-e",
        "BIGEYE_DEBUG=false",
        "bigeye-mcp-server:latest"
      ]
    }
  }
}
```

**Docker Flags Explained:**
- `-i`: Keep stdin open for communication with Claude Desktop
- `--rm`: Automatically remove the container when it stops (ephemeral)
- `-e`: Pass environment variables with your credentials

### Getting Your Credentials

1. **BIGEYE_API_KEY**: 
   - Log into your Bigeye instance
   - Navigate to Settings > API Keys
   - Create a new API key with appropriate permissions

2. **BIGEYE_API_URL**: 
   - Your Bigeye instance URL (e.g., `https://app.bigeye.com`, `https://demo.bigeye.com`)
   - Do not include trailing slashes

3. **BIGEYE_WORKSPACE_ID**: 
   - Found in your Bigeye URL after `/w/` (e.g., `https://app.bigeye.com/w/123/` → workspace ID is `123`)
   - Or navigate to Settings > Workspace in Bigeye

**Security Notes**:
- Never paste API keys directly into chat interfaces
- Store credentials securely in your Claude Desktop config
- Never commit credentials to version control

## Installation

### Quick Start with Claude Desktop

1. Build the Docker image locally:
   ```bash
   git clone https://github.com/your-org/bigeye-mcp-server.git
   cd bigeye-mcp-server
   docker build -t bigeye-mcp-server:latest .
   ```

2. Add the configuration to your Claude Desktop config file (see Configuration section above)

3. Replace the placeholder values with your actual Bigeye credentials

4. Restart Claude Desktop

The Docker container will spin up automatically when Claude Desktop needs it and terminate when no longer in use.

### Using Pre-built Docker Image

If a pre-built image is available on Docker Hub or GitHub Container Registry:

```json
{
  "mcpServers": {
    "bigeye": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "BIGEYE_API_KEY=your_api_key_here",
        "-e",
        "BIGEYE_API_URL=https://your-instance.bigeye.com",
        "-e",
        "BIGEYE_WORKSPACE_ID=your_workspace_id_here",
        "ghcr.io/your-org/bigeye-mcp-server:latest"
      ]
    }
  }
}
```

### Development Setup

For local development without Docker:

1. Install Python 3.12+
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set environment variables:
   ```bash
   export BIGEYE_API_KEY="your_api_key"
   export BIGEYE_API_URL="https://your-instance.bigeye.com"
   export BIGEYE_WORKSPACE_ID="your_workspace_id"
   ```
5. Run the server:
   ```bash
   python server.py
   ```

**Note**: For production use with Claude Desktop, always use the Docker approach for consistency and isolation.

## Available Tools

### Data Quality Management

- **`get_issues`** - Fetch data quality issues with filtering by status, schema names, and pagination
- **`get_table_issues`** - Get issues for a specific table
- **`analyze_table_data_quality`** - Comprehensive table quality analysis including metrics and issues
- **`update_issue`** - Update issue status, priority, or add comments
- **`merge_issues`** - Merge multiple issues into an incident
- **`unmerge_issues`** - Unmerge issues from incidents
- **`get_issue_resolution_steps`** - Get AI-powered resolution suggestions

### Data Lineage Analysis

- **`lineage_get_graph`** - Retrieve lineage graph for a data entity (upstream/downstream/bidirectional)
- **`lineage_get_node`** - Get details for a specific lineage node
- **`lineage_get_node_issues`** - Get all issues affecting a lineage node
- **`lineage_analyze_upstream_causes`** - Trace upstream to identify root causes of data issues
- **`lineage_analyze_downstream_impact`** - Analyze downstream impact of data issues
- **`lineage_trace_issue_path`** - Complete lineage trace from root cause to impact

### Agent Lineage Tracking

- **`lineage_track_data_access`** - Track which tables/columns an AI agent accesses
- **`lineage_commit_agent`** - Commit tracked access to Bigeye's lineage graph
- **`lineage_get_tracking_status`** - View current tracking status
- **`lineage_clear_tracked_assets`** - Clear tracking without committing
- **`lineage_cleanup_agent_edges`** - Clean up old agent lineage edges
- **`lineage_delete_node`** - Delete a custom lineage node (e.g., AI agent node)

### Catalog Exploration

- **`lineage_find_node`** - Find lineage nodes and get their IDs using advanced path-based search (supports wildcards, node type filtering, and custom node search)
- **`lineage_explore_catalog`** - Browse tables in Bigeye's catalog

### System Tools

- **`check_health`** - Check the health status of the Bigeye API

## Available Resources

- **`bigeye://auth/status`** - Current authentication status
- **`bigeye://issues/all`** - All issues from the configured workspace

## Available Prompts

- **`authentication_flow`** - Guide for setting up authentication
- **`check_connection_info`** - Guide for verifying API connection
- **`merge_issues_example`** - Examples for merging issues
- **`lineage_analysis_examples`** - Examples for lineage analysis

## Usage with Claude Desktop

1. Build the Docker image: `docker build -t bigeye-mcp-server:latest .`
2. Add the Bigeye MCP server configuration to your `claude_desktop_config.json` with your credentials
3. Restart Claude Desktop to load the new configuration
4. The server runs as an ephemeral container - starts when needed, stops when done
5. If credentials are missing or invalid, the container will exit with detailed setup instructions
6. Once configured correctly, use the tools to interact with Bigeye without exposing credentials in chat

**Container Lifecycle:**
- Container starts automatically when you begin using Bigeye tools
- Runs only while actively processing requests
- Automatically removed after stopping (no cleanup needed)
- Fresh instance starts for each session

## Agent Lineage Tracking

The Bigeye MCP server includes comprehensive lineage tracking for AI agents. This allows you to:

1. Track which data assets (tables/columns) an agent accesses across any data source
2. Create lineage relationships showing data flow from sources to the AI agent
3. Maintain a complete audit trail of agent data access
4. Clean up old lineage relationships based on retention policies

See [AGENT_LINEAGE_TRACKING.md](AGENT_LINEAGE_TRACKING.md) for detailed documentation.

## Troubleshooting

### Missing Environment Variables

If you see: `ERROR: Missing required environment variables`
- The server will display detailed instructions on how to configure your credentials
- Check your Claude Desktop config file contains all required environment variables
- Ensure variable names match exactly (case-sensitive)
- Verify the environment variables are properly formatted in the config
- Restart Claude Desktop after making config changes

### Authentication Errors

If authentication fails:
- Verify your API key is valid and has appropriate permissions
- Check that your workspace ID is correct (must be a number)
- Ensure your Bigeye instance URL is correct (no trailing slash)

### Connection Issues

If you can't connect to Bigeye:
- Check your network connection
- Verify the Bigeye instance URL is accessible
- Check for any firewall or proxy settings
- Enable debug mode with `BIGEYE_DEBUG=true`

## Security Best Practices

1. **Never** expose API keys in chat interfaces or logs
2. Use read-only API keys when possible
3. Rotate API keys regularly
4. Store `.env` files securely with restricted permissions
5. Use different API keys for different environments
6. Monitor API key usage in Bigeye

## Support

For issues or questions:
- Check the Bigeye documentation at https://docs.bigeye.com
- Contact your Bigeye administrator
- Open an issue in this repository