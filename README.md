# Bigeye MCP Server

An MCP (Model Context Protocol) server that provides tools for interacting with the Bigeye Data Observability platform.

## Features

- Query and manage data quality issues
- Analyze data lineage and dependencies
- Track AI agent data access patterns
- Perform root cause analysis for data quality issues
- Manage incidents and issue resolution

## 🔐 Security-First Configuration

**Important**: This server uses **environment variables only** for authentication. Dynamic authentication through the chat interface has been disabled to prevent credential exposure.

### Required Environment Variables

Create a `.env` file in the project directory with the following variables:

```bash
# Bigeye API Configuration
BIGEYE_API_URL=https://your-instance.bigeye.com
BIGEYE_API_KEY=your_api_key_here
BIGEYE_WORKSPACE_ID=your_workspace_id_here

# Optional
BIGEYE_DEBUG=false
```

**Security Notes**:
- Never paste API keys directly into Claude Desktop or any chat interface
- The `.env` file is excluded from Docker builds via `.dockerignore`
- Store credentials securely and never commit them to version control

### Example Configuration Files

See the provided examples:
- `.env.example` - Template for your configuration
- `.env.demo` - Example for demo environment
- `.env.app` - Example for production environment

## Installation

### Docker (Recommended)

1. Clone the repository
2. Create your `.env` file with the required variables
3. Build and run with Docker Compose:
   ```bash
   docker compose up --build
   ```

### Local Installation

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
4. Create your `.env` file
5. Run the server:
   ```bash
   python server.py
   ```

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

1. Set up your `.env` file with valid credentials
2. Configure Claude Desktop to use the MCP server
3. The server will automatically authenticate using your environment variables
4. Use the tools to interact with Bigeye without exposing credentials

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
- Ensure your `.env` file exists and contains all required variables
- Check that variable names match exactly (case-sensitive)
- Verify Docker Compose is reading the `.env` file

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