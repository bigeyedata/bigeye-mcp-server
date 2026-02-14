# Bigeye MCP Server

An MCP (Model Context Protocol) server that provides tools for interacting with the Bigeye Data Observability platform.

## Quick Start

1. Build the Docker image:
   ```bash
   ./build-docker.sh
   ```

2. Create a `.env` file with your credentials (see `.env.example`):
   ```bash
   cp .env.example .env
   # Edit .env with your values
   ```

3. Start the long-lived container:
   ```bash
   ./bigeye-mcp.sh start
   ```

4. Add the wrapper to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):
   ```json
   {
     "mcpServers": {
       "bigeye": {
         "command": "/absolute/path/to/mcp-wrapper.sh"
       }
     }
   }
   ```

5. Restart Claude Desktop.

### Getting Your Credentials

- **BIGEYE_API_KEY** — Generate in Bigeye under Settings > API Keys
- **BIGEYE_BASE_URL** — Your Bigeye instance URL (e.g. `https://app.bigeye.com`)
- **BIGEYE_WORKSPACE_ID** — Found in your Bigeye URL after `/w/` (e.g. `https://app.bigeye.com/w/123/` → `123`)

## Container Modes

### Long-Lived Container (Recommended)

Uses `mcp-wrapper.sh` + `bigeye-mcp.sh` with `docker compose`. The container stays running and Claude Desktop connects via `docker exec`.

```json
{
  "mcpServers": {
    "bigeye": {
      "command": "/absolute/path/to/mcp-wrapper.sh"
    }
  }
}
```

### Ephemeral Container

A fresh container spins up for each Claude Desktop session and is removed when done.

```json
{
  "mcpServers": {
    "bigeye": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-e", "BIGEYE_API_KEY=your_api_key_here",
        "-e", "BIGEYE_BASE_URL=https://app.bigeye.com",
        "-e", "BIGEYE_WORKSPACE_ID=your_workspace_id_here",
        "-e", "BIGEYE_DEBUG=false",
        "bigeye-mcp-server:latest"
      ]
    }
  }
}
```

## Container Management

The `bigeye-mcp.sh` script manages the long-lived container:

```bash
./bigeye-mcp.sh start     # Start the container
./bigeye-mcp.sh stop      # Stop the container
./bigeye-mcp.sh restart   # Restart the container
./bigeye-mcp.sh status    # Show container status
./bigeye-mcp.sh logs      # Follow container logs
./bigeye-mcp.sh rebuild   # Rebuild image and recreate container
./bigeye-mcp.sh clean     # Remove container and volumes
```

## Multi-Environment Setup

To connect to multiple Bigeye instances (e.g. demo and production), create separate environment files and compose overrides:

1. Create environment files for each instance:
   ```bash
   cp .env.example .env.demo
   cp .env.example .env.app
   # Edit each with the appropriate credentials
   ```

2. Use the environment-specific compose overrides:
   ```bash
   # Demo
   docker compose -f docker-compose.yml -f docker-compose.demo.yml --env-file .env.demo up -d bigeye-mcp-demo

   # Production
   docker compose -f docker-compose.yml -f docker-compose.app.yml --env-file .env.app up -d bigeye-mcp-app
   ```

3. Add both to your Claude Desktop config:
   ```json
   {
     "mcpServers": {
       "bigeye-demo": {
         "command": "docker",
         "args": [
           "run", "--rm", "-i",
           "-e", "BIGEYE_API_KEY=your_demo_key",
           "-e", "BIGEYE_BASE_URL=https://demo.bigeye.com",
           "-e", "BIGEYE_WORKSPACE_ID=your_demo_workspace_id",
           "bigeye-mcp-server:latest"
         ]
       },
       "bigeye-app": {
         "command": "docker",
         "args": [
           "run", "--rm", "-i",
           "-e", "BIGEYE_API_KEY=your_app_key",
           "-e", "BIGEYE_BASE_URL=https://app.bigeye.com",
           "-e", "BIGEYE_WORKSPACE_ID=your_app_workspace_id",
           "bigeye-mcp-server:latest"
         ]
       }
     }
   }
   ```

## Available Tools

### Issue Management

- **`list_issues`** — List data quality issues across the workspace
- **`get_issue`** — Get full details for a single issue by its internal ID
- **`search_issues`** — Find issues by their display name/number (e.g. "10921")
- **`list_related_issues`** — List issues related to a given issue via lineage
- **`list_table_issues`** — List data quality issues for a specific table by name
- **`update_issue`** — Update an issue's status, priority, or add a timeline message
- **`create_incident`** — Create an incident by merging related issues
- **`delete_incident_members`** — Remove issues from an incident
- **`get_resolution_steps`** — Get recommended resolution steps for an issue

### Metrics & Quality

- **`list_table_metrics`** — List all metrics (monitors) configured on a table
- **`get_table_quality_report`** — Comprehensive quality report: catalog metadata + metrics + active issues
- **`get_table_profile`** — Get data profile report including column statistics and distribution
- **`create_profile_job`** — Queue a new data profiling job for a table
- **`get_profile_job_status`** — Check the status of a profiling job

### Catalog Search

- **`search_schemas`** — Search for schemas by name (partial matching)
- **`search_tables`** — Search for tables by exact or partial name match
- **`search_columns`** — Search for columns by name (partial matching)

### Data Lineage

- **`get_lineage_graph`** — Get the full lineage graph (upstream/downstream/both) from a starting node
- **`get_lineage_node`** — Get details for a specific lineage node
- **`list_lineage_node_issues`** — List issues for a lineage node by its node ID
- **`search_lineage_nodes`** — Find lineage node IDs by path pattern (e.g. "WAREHOUSE/SCHEMA/TABLE")
- **`lineage_explore_catalog`** — Explore tables in Bigeye's catalog
- **`lineage_delete_node`** — Delete a custom lineage node

### Root Cause & Impact Analysis

- **`get_upstream_root_causes`** — Analyze upstream lineage to identify root causes of issues
- **`get_downstream_impact`** — Analyze downstream impact of issues at a lineage node
- **`get_issue_lineage_trace`** — Trace a data quality issue end-to-end through lineage
- **`list_report_upstream_issues`** — List upstream issues affecting a BI report or dashboard

### Agent Lineage Tracking

- **`lineage_track_data_access`** — Track data assets accessed by an AI agent
- **`lineage_commit_agent`** — Commit tracked data access to Bigeye's lineage graph
- **`lineage_get_tracking_status`** — Get the current status of lineage tracking
- **`lineage_clear_tracked_assets`** — Clear all tracked data assets without committing
- **`lineage_cleanup_agent_edges`** — Clean up old lineage edges for the AI agent

### System

- **`get_health_status`** — Check the health and connectivity of the Bigeye API
- **`list_resources`** — List all available MCP resources
- **`list_data_sources`** — List all data sources/warehouses connected to Bigeye

## Resources & Prompts

**Resources:**
- `bigeye://auth/status` — Current authentication status
- `bigeye://issues/all` — All issues from the configured workspace

**Prompts:**
- `authentication_flow` — Guide for setting up authentication
- `check_connection_info` — Guide for verifying API connection
- `merge_issues_example` — Examples for merging issues
- `lineage_analysis_examples` — Examples for lineage analysis

## Agent Lineage Tracking

The server includes comprehensive lineage tracking for AI agents — see [AGENT_LINEAGE_TRACKING.md](AGENT_LINEAGE_TRACKING.md) for details.

## Development Setup

For local development without Docker:

1. Install Python 3.12+
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set environment variables:
   ```bash
   export BIGEYE_API_KEY="your_api_key"
   export BIGEYE_BASE_URL="https://app.bigeye.com"
   export BIGEYE_WORKSPACE_ID="your_workspace_id"
   ```
5. Run the server:
   ```bash
   python server.py
   ```

## Troubleshooting

### Missing Environment Variables
- Check your `.env` file or Claude Desktop config contains all required variables
- Variable names are case-sensitive
- Restart Claude Desktop after config changes

### Authentication Errors
- Verify your API key is valid with appropriate permissions
- Workspace ID must be a number
- Instance URL should have no trailing slash

### Connection Issues
- Verify the Bigeye instance URL is accessible
- Check for firewall/proxy settings
- Enable debug mode: `BIGEYE_DEBUG=true`

### Container Not Starting
- Check Docker is running: `docker info`
- Verify image exists: `docker images | grep bigeye`
- Check logs: `./bigeye-mcp.sh logs`
