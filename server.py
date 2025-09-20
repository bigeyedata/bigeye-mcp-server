"""
MCP Server for Bigeye API

This server connects to the Bigeye Datawatch API and exposes resources and tools
for interacting with data quality monitoring. Credentials are provided via
environment variables from Claude Desktop configuration.
"""

from mcp.server.fastmcp import FastMCP, Context
import os
import sys
import json
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime, timedelta

# Import our modules
from auth import BigeyeAuthClient
from bigeye_api import BigeyeAPIClient
from config import config
from lineage_tracker import AgentLineageTracker

# Create an MCP server with system instructions
mcp = FastMCP(
    "Bigeye API",
    instructions="""
    Bigeye Data Observability Platform Integration
    
    This server provides both RESOURCES and TOOLS for data quality monitoring:
    
    RESOURCES (Read-only, Fast Access)
    ===================================
    Resources provide quick access to frequently needed data. Use these for:
    - Checking current issue status: bigeye://issues/active
    - Reviewing recent activity: bigeye://issues/recent
    - Getting configuration info: bigeye://config
    
    Use the list_resources() tool to discover all available resources.
    Resources are ideal for dashboards, status checks, and quick queries.
    
    TOOLS (Actions and Complex Queries)
    ====================================
    Tools perform actions and complex filtering:
    - Query and manage data quality issues
    - Analyze data lineage and dependencies
    - Track AI agent data access patterns
    - Perform root cause analysis
    
    The server is pre-configured with credentials from environment variables.
    
    IMPORTANT: Choosing Resources vs Tools
    =======================================
    - Use RESOURCES for: Quick status checks, common queries, dashboard data
    - Use TOOLS for: Specific filtering, actions, updates, complex analysis
    
    Example: "Show me active issues" → Use resource bigeye://issues/active
    Example: "Show issues for schema X" → Use get_issues() tool with filters
    
    IMPORTANT: Table and Column Search Workflow
    ============================================
    When a user asks about a specific table, column, or schema by name:
    
    1. ALWAYS search first using the appropriate search tool:
       - Use search_tables() when asked about a table
       - Use search_columns() when asked about a column
       - Use search_schemas() when asked about a schema
    
    2. Present the search results to the user as a numbered list, showing:
       - Full qualified name (e.g., ORACLE.PROD_SCHEMA.ORDERS)
       - Database system it belongs to
       - Any relevant metadata (row count, column count, etc.)
    
    3. Ask the user to confirm which specific object they meant by number or name
    
    4. Only after the user confirms the specific object should you proceed with 
       the rest of their request (checking health, analyzing issues, etc.)
    
    5. ALWAYS refer to tables and columns by their FULL QUALIFIED NAME in all
       communications with the user. Never say just "the ORDERS table" - say
       "the ORACLE.PROD_SCHEMA.ORDERS table" to be clear about which database
       system it belongs to.
    
    Example interaction:
    User: "Check the health of the orders table"
    Assistant: "I found 3 tables with 'orders' in the name:
                1. ORACLE.PROD_SCHEMA.ORDERS (in Oracle database)
                2. SNOWFLAKE.ANALYTICS.ORDERS (in Snowflake database)  
                3. POSTGRES.PUBLIC.ORDERS (in Postgres database)
                Which one would you like me to check?"
    
    This ensures accuracy and prevents operations on the wrong database objects.
    """
)

# Debug function
def debug_print(message: str):
    """Print debug messages to stderr"""
    if config["debug"] or os.environ.get("BIGEYE_DEBUG", "false").lower() in ["true", "1", "yes"]:
        print(f"[BIGEYE MCP DEBUG] {message}", file=sys.stderr)

# Initialize clients
auth_client = BigeyeAuthClient()
api_client = None
lineage_tracker = None

# Initialize with configured credentials
debug_print(f"Using configured authentication: {config['api_url']}")
debug_print(f"Workspace ID from config: {config.get('workspace_id')}")
if config.get("workspace_id") and config.get("api_key"):
    auth_client.set_credentials(
        config["api_url"],
        config["workspace_id"],
        config["api_key"]
    )
    debug_print(f"Auth client initialized with workspace ID: {config.get('workspace_id')}")
api_client = BigeyeAPIClient(
    api_url=config["api_url"],
    api_key=config["api_key"],
    workspace_id=config.get("workspace_id")
)
if config.get("workspace_id"):
    lineage_tracker = AgentLineageTracker(
        bigeye_client=api_client,
        workspace_id=config["workspace_id"],
        debug=config.get("debug", False)
    )

def get_api_client() -> BigeyeAPIClient:
    """Get the API client"""
    return api_client

# Authentication status resource
@mcp.resource("bigeye://auth/status")
async def auth_status() -> str:
    """Current authentication status"""
    workspace_id = config.get('workspace_id')
    if not workspace_id or not config.get('api_key'):
        return """ERROR: Bigeye credentials not configured.
        
Please configure credentials in your Claude Desktop config file.
See README for setup instructions."""
    
    return f"""Connected to Bigeye:
- Instance: {config['api_url']}
- Workspace ID: {workspace_id}
- Status: ✓ Authenticated via environment variables"""

# Note: Dynamic authentication has been removed.
# Credentials must be provided via environment variables.


# Workspace switching removed - use environment variables




# Resources
@mcp.resource("bigeye://health")
async def get_health_resource() -> str:
    """Get the health status of the Bigeye API."""
    client = get_api_client()
    try:
        result = await client.check_health()
        return f"API Health Status: {result.get('status', 'Unknown')}"
    except Exception as e:
        return f"Error checking API health: {str(e)}"

@mcp.resource("bigeye://config")
def get_config_resource() -> Dict[str, Any]:
    """Get the current configuration for the Bigeye API connector."""
    return {
        "authenticated": bool(config.get('api_key')),
        "instance": config['api_url'],
        "workspace_id": config.get('workspace_id'),
        "api_base_url": f"{config['api_url']}/api/v1"
    }

@mcp.resource("bigeye://issues")
async def get_issues_resource() -> Dict[str, Any]:
    """Get all issues from the configured workspace."""
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    if not workspace_id:
        return {"error": "No workspace ID configured"}
    
    debug_print(f"Fetching all issues for workspace {workspace_id}")
    result = await client.fetch_issues(workspace_id=workspace_id)
    
    issue_count = len(result.get("issues", []))
    debug_print(f"Found {issue_count} issues")
    
    return result

@mcp.resource("bigeye://issues/active")
async def get_active_issues_resource() -> Dict[str, Any]:
    """Get currently active data quality issues.
    
    Returns only issues with status NEW or ACKNOWLEDGED, excluding closed and merged issues.
    Provides a focused view of current problems that need attention.
    """
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    if not workspace_id:
        return {"error": "No workspace ID configured"}
    
    debug_print(f"Fetching active issues for workspace {workspace_id}")
    
    # Fetch only NEW and ACKNOWLEDGED issues
    result = await client.fetch_issues(
        workspace_id=workspace_id,
        currentStatus=["ISSUE_STATUS_NEW", "ISSUE_STATUS_ACKNOWLEDGED"],
        page_size=50,  # Limit to most recent 50 active issues
        include_full_history=False  # Keep response size manageable
    )
    
    issues = result.get("issues", [])
    
    # Organize issues by severity and table
    organized = {
        "summary": {
            "total_active": len(issues),
            "by_status": {},
            "by_priority": {},
            "by_schema": {},
            "most_affected_tables": []
        },
        "issues": [],
        "last_updated": datetime.now().isoformat()
    }
    
    # Count by status and priority
    status_counts = {}
    priority_counts = {}
    schema_counts = {}
    table_counts = {}
    
    for issue in issues:
        # Status counting
        status = issue.get("currentStatus", "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        
        # Priority counting
        priority = issue.get("priority", "UNKNOWN")
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        
        # Schema counting
        schema = issue.get("schemaName", "UNKNOWN")
        schema_counts[schema] = schema_counts.get(schema, 0) + 1
        
        # Table counting
        table = issue.get("tableName", "UNKNOWN")
        if table != "UNKNOWN":
            table_counts[table] = table_counts.get(table, 0) + 1
        
        # Build full qualified name for the table
        warehouse = issue.get("warehouseName", "")
        database = issue.get("databaseName", "")
        full_table_parts = [p for p in [warehouse, database, schema, table] if p]
        full_table_name = ".".join(full_table_parts) if full_table_parts else table
        
        # Add simplified issue to list
        organized["issues"].append({
            "id": issue.get("id"),
            "name": issue.get("name"),
            "status": status,
            "priority": priority,
            "table": table,
            "schema": schema,
            "warehouse": warehouse,
            "database": database,
            "full_table_name": full_table_name,
            "display_table": f"{full_table_name} ({warehouse or database} database)" if (warehouse or database) else table,
            "metric": issue.get("metric", {}).get("name") if issue.get("metric") else None,
            "created_at": issue.get("createdAt"),
            "last_event_time": issue.get("lastEventTime"),
            "description": issue.get("description")
        })
    
    # Update summary
    organized["summary"]["by_status"] = status_counts
    organized["summary"]["by_priority"] = priority_counts
    organized["summary"]["by_schema"] = schema_counts
    organized["summary"]["most_affected_tables"] = sorted(
        table_counts.items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:5]  # Top 5 most affected tables
    
    debug_print(f"Found {len(issues)} active issues")
    
    return organized

@mcp.resource("bigeye://issues/recent")
async def get_recent_issues_resource() -> Dict[str, Any]:
    """Get recently updated or resolved issues.
    
    Returns issues that have been updated in the last 7 days, including resolved ones,
    to help track resolution patterns and recent activity.
    """
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    if not workspace_id:
        return {"error": "No workspace ID configured"}
    
    debug_print(f"Fetching recent issues for workspace {workspace_id}")
    
    # Fetch all issues (we'll filter by date client-side since the API doesn't have date filters)
    result = await client.fetch_issues(
        workspace_id=workspace_id,
        page_size=100,  # Get more issues to ensure we have recent ones
        include_full_history=False
    )
    
    issues = result.get("issues", [])
    
    # Calculate 7 days ago timestamp
    seven_days_ago = datetime.now() - timedelta(days=7)
    seven_days_ago_ms = int(seven_days_ago.timestamp() * 1000)
    
    # Filter for recently updated issues
    recent_issues = []
    for issue in issues:
        # Check if updated recently (using lastEventTime or updatedAt)
        last_event = issue.get("lastEventTime", 0)
        updated_at = issue.get("updatedAt", 0)
        most_recent = max(last_event, updated_at)
        
        if most_recent >= seven_days_ago_ms:
            recent_issues.append(issue)
    
    # Organize by resolution status
    organized = {
        "summary": {
            "total_recent": len(recent_issues),
            "resolved_count": 0,
            "new_count": 0,
            "acknowledged_count": 0,
            "resolution_rate": 0.0,
            "average_resolution_time_hours": None
        },
        "resolved": [],
        "new_issues": [],
        "still_active": [],
        "timeline": [],
        "last_updated": datetime.now().isoformat()
    }
    
    resolution_times = []
    
    for issue in recent_issues:
        status = issue.get("currentStatus", "")
        
        # Build full qualified name for the table
        warehouse = issue.get("warehouseName", "")
        schema = issue.get("schemaName", "")
        table = issue.get("tableName", "")
        column = issue.get("columnName", "")
        
        full_table_name = ""
        if warehouse and schema and table:
            full_table_name = f"{warehouse}.{schema}.{table}"
        elif schema and table:
            full_table_name = f"{schema}.{table}"
        elif table:
            full_table_name = table
            
        display_table = f"{full_table_name} ({warehouse} database)" if warehouse and full_table_name else full_table_name
        
        simplified_issue = {
            "full_table_name": full_table_name,
            "display_table": display_table,
            "USE_THIS_TABLE_NAME": full_table_name,
            "id": issue.get("id"),
            "name": issue.get("name"),
            "status": status,
            "priority": issue.get("priority"),
            "table": issue.get("tableName"),
            "schema": issue.get("schemaName"),
            "created_at": issue.get("createdAt"),
            "last_event_time": issue.get("lastEventTime"),
            "metric": issue.get("metric", {}).get("name") if issue.get("metric") else None
        }
        
        # Categorize by status
        if status == "ISSUE_STATUS_CLOSED":
            organized["resolved"].append(simplified_issue)
            organized["summary"]["resolved_count"] += 1
            
            # Calculate resolution time if we have both created and resolved times
            if issue.get("createdAt") and issue.get("lastEventTime"):
                resolution_time_ms = issue["lastEventTime"] - issue["createdAt"]
                resolution_times.append(resolution_time_ms / (1000 * 60 * 60))  # Convert to hours
                
        elif status == "ISSUE_STATUS_NEW":
            # Check if it was created in the last 7 days
            if issue.get("createdAt", 0) >= seven_days_ago_ms:
                organized["new_issues"].append(simplified_issue)
                organized["summary"]["new_count"] += 1
            else:
                organized["still_active"].append(simplified_issue)
                
        elif status in ["ISSUE_STATUS_ACKNOWLEDGED", "ISSUE_STATUS_MONITORING"]:
            organized["still_active"].append(simplified_issue)
            if status == "ISSUE_STATUS_ACKNOWLEDGED":
                organized["summary"]["acknowledged_count"] += 1
        
        # Add to timeline
        organized["timeline"].append({
            "timestamp": issue.get("lastEventTime"),
            "issue_id": issue.get("id"),
            "issue_name": issue.get("name"),
            "event": f"Issue {status}",
            "table": issue.get("tableName")
        })
    
    # Sort timeline by timestamp (most recent first)
    organized["timeline"] = sorted(
        organized["timeline"], 
        key=lambda x: x["timestamp"] if x["timestamp"] else 0, 
        reverse=True
    )[:20]  # Keep only 20 most recent events
    
    # Calculate resolution rate
    if organized["summary"]["total_recent"] > 0:
        organized["summary"]["resolution_rate"] = round(
            (organized["summary"]["resolved_count"] / organized["summary"]["total_recent"]) * 100, 
            1
        )
    
    # Calculate average resolution time
    if resolution_times:
        organized["summary"]["average_resolution_time_hours"] = round(
            sum(resolution_times) / len(resolution_times), 
            1
        )
    
    debug_print(f"Found {len(recent_issues)} recent issues (last 7 days)")
    
    return organized

# Tools
@mcp.tool()
async def list_resources() -> Dict[str, Any]:
    """List all available MCP resources for quick data access.
    
    Resources provide read-only, cacheable access to frequently needed data.
    Use resources instead of tools when you need quick reference information
    that doesn't require complex filtering or actions.
    
    Returns:
        Dictionary containing available resources with their URIs and descriptions
    """
    return {
        "description": "Available MCP resources for quick data access",
        "resources": [
            {
                "uri": "bigeye://auth/status",
                "description": "Check authentication status",
                "update_frequency": "On demand"
            },
            {
                "uri": "bigeye://health",
                "description": "API health check status",
                "update_frequency": "On demand"
            },
            {
                "uri": "bigeye://config",
                "description": "Current configuration (workspace, API URL)",
                "update_frequency": "Static"
            },
            {
                "uri": "bigeye://issues",
                "description": "All issues in the workspace (can be large)",
                "update_frequency": "5 minutes",
                "note": "Consider using /active or /recent for filtered views"
            },
            {
                "uri": "bigeye://issues/active",
                "description": "Currently active issues (NEW and ACKNOWLEDGED only)",
                "update_frequency": "5 minutes",
                "features": [
                    "Summary statistics by status, priority, schema",
                    "Top 5 most affected tables",
                    "Simplified issue format"
                ]
            },
            {
                "uri": "bigeye://issues/recent",
                "description": "Issues updated in the last 7 days",
                "update_frequency": "15 minutes",
                "features": [
                    "Resolution rate and average resolution time",
                    "Timeline of recent events",
                    "Categorized by resolved/new/active"
                ]
            }
        ],
        "usage_tip": "Access resources using their URI, e.g., 'Show me bigeye://issues/active'"
    }

@mcp.tool()
async def check_health() -> Dict[str, Any]:
    """Check the health of the Bigeye API."""
    
    client = get_api_client()
    debug_print("Checking API health")
    result = await client.check_health()
    debug_print(f"Health check result: {result}")
    return result

@mcp.tool()
async def get_issues(
    statuses: Optional[List[str]] = None,
    schema_names: Optional[List[str]] = None,
    page_size: Optional[int] = None,
    page_cursor: Optional[str] = None
) -> Dict[str, Any]:
    """Get issues from the Bigeye API with optimized response size.
    
    NOTE: For quick access to common issue queries, consider using these resources instead:
    - bigeye://issues/active - Returns only NEW and ACKNOWLEDGED issues with summaries
    - bigeye://issues/recent - Returns issues from last 7 days with resolution metrics
    
    This tool is best for custom filtering by specific statuses or schemas.
    It fetches issues with only essential metadata and minimal event history.
    
    Args:
        statuses: Optional list of issue statuses to filter by. Possible values:
            - ISSUE_STATUS_NEW
            - ISSUE_STATUS_ACKNOWLEDGED
            - ISSUE_STATUS_CLOSED
            - ISSUE_STATUS_MONITORING
            - ISSUE_STATUS_MERGED
        schema_names: Optional list of schema names to filter issues by
        page_size: Optional number of issues to return per page (default: 20)
        page_cursor: Cursor for pagination
        
    Returns:
        Dictionary containing issues with essential metadata only
    """
    
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    
    # Safety check
    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration'
        }
    
    debug_print(f"Fetching issues for workspace {workspace_id}")
    debug_print(f"Config state - Instance: {config['api_url']}, Workspace: {workspace_id}, Has API key: {bool(config.get('api_key'))}")
    
    if statuses:
        debug_print(f"Filtering by statuses: {statuses}")
    if schema_names:
        debug_print(f"Filtering by schema names: {schema_names}")
        
    result = await client.fetch_issues(
        workspace_id=workspace_id,  # Use the variable we captured above
        currentStatus=statuses,
        schemaNames=schema_names,
        page_size=page_size if page_size else 20,  # Default to 20 issues
        page_cursor=page_cursor,
        include_full_history=False  # Exclude full metric run history
    )
    
    issue_count = len(result.get("issues", []))
    debug_print(f"Found {issue_count} issues")
    
    return result

@mcp.tool()
async def get_table_issues(
    table_name: str,
    warehouse_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    statuses: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Get data quality issues for a specific table.
    
    IMPORTANT: Before using this tool, you should use search_tables() to find and confirm
    the exact table the user is referring to. Only use this tool AFTER the user has
    confirmed which specific table they mean.
    
    IMPORTANT: When reporting issues to the user, always use the FULL QUALIFIED NAME
    of the table (e.g., "ORACLE.PROD_SCHEMA.ORDERS" not just "ORDERS table").
    
    NOTE: For general issue monitoring, consider using these resources instead:
    - bigeye://issues/active - For current active issues across all tables
    - bigeye://issues/recent - For recent issue activity and resolution patterns
    
    This tool fetches all issues related to a specific table in Bigeye,
    making it easier to check data quality for individual tables.
    
    Args:
        table_name: Name of the table (e.g., "ORDERS")
        warehouse_name: Optional warehouse name (e.g., "ORACLE", "SNOWFLAKE")
        schema_name: Optional schema name (e.g., "PROD_REPL")
        statuses: Optional list of issue statuses to filter by:
            - ISSUE_STATUS_NEW
            - ISSUE_STATUS_ACKNOWLEDGED
            - ISSUE_STATUS_CLOSED
            - ISSUE_STATUS_MONITORING
            - ISSUE_STATUS_MERGED
            
    Returns:
        Dictionary containing issues for the specific table
        
    Example:
        # Get all issues for the ORDERS table
        await get_table_issues(table_name="ORDERS")
        
        # Get only new issues for ORDERS table in PROD_REPL schema
        await get_table_issues(
            table_name="ORDERS",
            schema_name="PROD_REPL",
            statuses=["ISSUE_STATUS_NEW"]
        )
    """
    
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    
    # Safety check
    if not workspace_id:
        return {
            'error': 'Workspace ID not set',
            'hint': 'Authentication may be incomplete. Try re-authenticating.'
        }
    
    debug_print(f"Fetching issues for table {table_name} in workspace {workspace_id}")
    
    try:
        result = await client.get_issues_for_table(
            workspace_id=workspace_id,
            table_name=table_name,
            warehouse_name=warehouse_name,
            schema_name=schema_name,
            currentStatus=statuses
        )
        
        if result.get("error"):
            return result
            
        # Add summary information
        total_issues = result.get("total_issues", 0)
        if total_issues > 0:
            # Group issues by status
            status_counts = {}
            for issue in result.get("issues", []):
                status = issue.get("currentStatus", "UNKNOWN")
                status_counts[status] = status_counts.get(status, 0) + 1
                
            result["summary"] = {
                "total_issues": total_issues,
                "by_status": status_counts
            }
            
            debug_print(f"Found {total_issues} issues for table {table_name}")
        else:
            debug_print(f"No issues found for table {table_name}")
            
        return result
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching table issues: {str(e)}"
        }

@mcp.tool()
async def analyze_table_data_quality(
    table_name: str,
    schema_name: Optional[str] = None,
    warehouse_name: Optional[str] = None
) -> Dict[str, Any]:
    """Analyze data quality for a specific table including issues and metrics.
    
    IMPORTANT: Before using this tool, you MUST use search_tables() first to find 
    and confirm the exact table the user is referring to. Only use this tool AFTER 
    the user has confirmed which specific table they mean.
    
    This comprehensive tool checks:
    1. If the table exists in Bigeye's catalog
    2. What data quality metrics are configured
    3. What issues (if any) exist for the table
    
    Args:
        table_name: Name of the table to analyze (e.g., "ORDERS")
        schema_name: Optional schema name (e.g., "PROD_REPL")
        warehouse_name: Optional warehouse name (e.g., "SNOWFLAKE")
        
    Returns:
        Comprehensive data quality analysis for the table
        
    Example:
        # Analyze the ORDERS table
        await analyze_table_data_quality(
            table_name="ORDERS",
            schema_name="PROD_REPL"
        )
    """
    
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    
    if not workspace_id:
        return {
            'error': 'Workspace ID not set',
            'hint': 'Authentication may be incomplete. Try re-authenticating.'
        }
    
    debug_print(f"Analyzing data quality for table {table_name}")
    
    try:
        # First, check if table exists in catalog
        catalog_result = await client.get_catalog_tables(
            workspace_id=workspace_id,
            schema_name=schema_name,
            warehouse_name=warehouse_name,
            page_size=100
        )
        
        if catalog_result.get("error"):
            return {
                "error": True,
                "message": "Failed to check catalog",
                "details": catalog_result
            }
            
        tables = catalog_result.get("tables", [])
        matching_table = None
        
        for table in tables:
            if table.get("tableName", "").upper() == table_name.upper():
                matching_table = table
                break
                
        if not matching_table:
            # Table not found - provide helpful info
            available_tables = [t.get("tableName") for t in tables]
            return {
                "error": True,
                "message": f"Table {table_name} not found in Bigeye catalog",
                "available_tables_in_schema": available_tables[:10],  # Show first 10
                "hint": "Make sure the table name is correct and has been imported into Bigeye"
            }
            
        # Get the table details with full qualified name
        warehouse = matching_table.get("warehouseName", "")
        schema = matching_table.get("schemaName", "")
        table = matching_table.get("tableName", "")
        
        full_qualified_name = ""
        if warehouse and schema and table:
            full_qualified_name = f"{warehouse}.{schema}.{table}"
        elif schema and table:
            full_qualified_name = f"{schema}.{table}"
        elif table:
            full_qualified_name = table
            
        table_info = {
            "full_qualified_name": full_qualified_name,
            "USE_THIS_NAME": full_qualified_name,
            "display_name": f"{full_qualified_name} (in {warehouse} database)" if warehouse else full_qualified_name,
            "table_name": table,
            "schema_name": schema,
            "warehouse_name": warehouse,
            "table_id": matching_table.get("id")
        }
        
        # Get issues for the table
        issues_result = await client.get_issues_for_table(
            workspace_id=workspace_id,
            table_name=table_name,
            warehouse_name=warehouse_name,
            schema_name=schema_name or matching_table.get("schemaName")
        )
        
        # Get metrics for the table
        metrics_result = await client.get_table_metrics(
            workspace_id=workspace_id,
            table_name=table_name,
            schema_name=schema_name or matching_table.get("schemaName")
        )
        
        # Compile the analysis
        analysis = {
            "table": table_info,
            "data_quality_summary": {
                "total_issues": issues_result.get("total_issues", 0),
                "issues_by_status": {},
                "has_metrics": not metrics_result.get("error")
            },
            "issues": issues_result.get("issues", []),
            "metrics": metrics_result if not metrics_result.get("error") else None
        }
        
        # Group issues by status
        for issue in issues_result.get("issues", []):
            status = issue.get("currentStatus", "UNKNOWN")
            analysis["data_quality_summary"]["issues_by_status"][status] = \
                analysis["data_quality_summary"]["issues_by_status"].get(status, 0) + 1
                
        debug_print(f"Analysis complete for {table_name}: {analysis['data_quality_summary']['total_issues']} issues found")
        
        return analysis
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error analyzing table data quality: {str(e)}"
        }

@mcp.tool()
async def merge_issues(
    issue_ids: List[int],
    existing_incident_id: Optional[int] = None,
    incident_name: Optional[str] = None
) -> Dict[str, Any]:
    """Merge multiple issues into a single incident.
    
    
    This tool can either create a new incident or merge issues into an existing incident.
    
    Args:
        issue_ids: List of issue IDs to merge (must contain at least 2 issues when creating new incident, 
                  or at least 1 when merging into existing incident)
        existing_incident_id: Optional ID of an existing incident to merge issues into.
                             If not provided, a new incident will be created.
        incident_name: Optional name for the incident (applies to both new and existing incidents)
        
    Returns:
        Dictionary containing the merge response with the created/updated incident
    """
    
    # Validation
    min_issues = 1 if existing_incident_id else 2
    if not issue_ids or len(issue_ids) < min_issues:
        if existing_incident_id:
            error_msg = "At least 1 issue ID is required when merging into an existing incident"
        else:
            error_msg = "At least 2 issue IDs are required when creating a new incident"
        return {
            "error": True,
            "message": error_msg
        }
    
    client = get_api_client()
    if existing_incident_id:
        debug_print(f"Merging issues {issue_ids} into existing incident {existing_incident_id}")
    else:
        debug_print(f"Creating new incident from issues {issue_ids}")
    
    try:
        result = await client.merge_issues(
            issue_ids=issue_ids,
            workspace_id=config.get('workspace_id'),
            existing_incident_id=existing_incident_id,
            incident_name=incident_name
        )
        
        debug_print(f"Merge response: {result}")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error merging issues: {str(e)}"
        }

@mcp.tool()
async def get_issue_resolution_steps(
    issue_id: int
) -> Dict[str, Any]:
    """Get resolution steps for an issue or incident.
    
    This tool fetches the current resolution steps for an issue or incident. 
    These steps provide guidance on how to resolve the data quality issue.
    
    Args:
        issue_id: The ID of the issue or incident to get resolution steps for
        
    Returns:
        Dictionary containing the resolution steps for the issue
    """
    
    client = get_api_client()
    debug_print(f"Fetching resolution steps for issue ID: {issue_id}")
    
    try:
        result = await client.get_issue_resolution_steps(issue_id=issue_id)
        step_count = len(result.get("steps", []))
        debug_print(f"Found {step_count} resolution steps for issue {issue_id}")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching resolution steps: {str(e)}"
        }

@mcp.tool()
async def update_issue(
    issue_id: int,
    new_status: Optional[str] = None,
    closing_label: Optional[str] = None,
    priority: Optional[str] = None,
    message: Optional[str] = None
) -> Dict[str, Any]:
    """Update an issue with status, priority, and/or add a timeline message.
    
    This tool allows you to update various aspects of an issue in a single request.
    
    Args:
        issue_id: The ID of the issue to update
        new_status: New status for the issue. Possible values:
            - ISSUE_STATUS_NEW
            - ISSUE_STATUS_ACKNOWLEDGED  
            - ISSUE_STATUS_CLOSED
            - ISSUE_STATUS_MONITORING
            - ISSUE_STATUS_MERGED
        closing_label: Required when new_status is ISSUE_STATUS_CLOSED. Possible values:
            - METRIC_RUN_LABEL_TRUE_NEGATIVE
            - METRIC_RUN_LABEL_FALSE_NEGATIVE
            - METRIC_RUN_LABEL_TRUE_POSITIVE
            - METRIC_RUN_LABEL_FALSE_POSITIVE
        priority: New priority for the issue. Possible values:
            - ISSUE_PRIORITY_LOW
            - ISSUE_PRIORITY_MED
            - ISSUE_PRIORITY_HIGH
        message: Timeline message to add to the issue
        
    Returns:
        Dictionary containing the API response with the updated issue information
    """
    
    # Validation
    if new_status == "ISSUE_STATUS_CLOSED" and not closing_label:
        return {
            "error": True,
            "message": "closing_label is required when new_status is ISSUE_STATUS_CLOSED"
        }
    
    if not any([new_status, closing_label, priority, message]):
        return {
            "error": True,
            "message": "At least one update parameter must be provided"
        }
    
    client = get_api_client()
    debug_print(f"Updating issue ID: {issue_id}")
    
    try:
        result = await client.update_issue(
            issue_id=issue_id,
            new_status=new_status,
            closing_label=closing_label,
            priority=priority,
            message=message
        )
        debug_print(f"Issue {issue_id} updated successfully")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error updating issue: {str(e)}"
        }

@mcp.tool()
async def unmerge_issues(
    issue_ids: Optional[List[int]] = None,
    parent_issue_ids: Optional[List[int]] = None,
    assignee_id: Optional[int] = None,
    new_status: Optional[str] = None
) -> Dict[str, Any]:
    """Unmerge issues from incidents they have been merged into.
    
    This tool removes issues from incidents, making them independent issues again.
    
    Args:
        issue_ids: Optional list of specific issue IDs to unmerge from their incidents
        parent_issue_ids: Optional list of incident IDs to unmerge all child issues from
        assignee_id: Optional user ID to assign the unmerged issues to
        new_status: Optional new status for the unmerged issues
        
    Returns:
        Dictionary containing the unmerge response with success/failure details
    """
    
    # Validation
    if not issue_ids and not parent_issue_ids:
        return {
            "error": True,
            "message": "Either issue_ids or parent_issue_ids must be provided"
        }
    
    client = get_api_client()
    debug_print(f"Unmerging issues in workspace {config.get('workspace_id')}")
    
    try:
        result = await client.unmerge_issues(
            workspace_id=config.get('workspace_id'),
            issue_ids=issue_ids,
            parent_issue_ids=parent_issue_ids,
            assignee_id=assignee_id,
            new_status=new_status
        )
        debug_print(f"Unmerge response: {result}")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error unmerging issues: {str(e)}"
        }

@mcp.tool()
async def lineage_get_graph(
    node_id: int,
    direction: str = "bidirectional",
    max_depth: Optional[int] = None,
    include_issues: bool = True
) -> Dict[str, Any]:
    """Get the complete lineage graph for a data entity.
    
    This tool retrieves the data lineage graph for a specific node, showing all upstream
    and/or downstream dependencies.
    
    Args:
        node_id: The ID of the lineage node (table, column, etc.) to analyze
        direction: Direction to traverse the lineage graph:
            - "bidirectional" (default): Get both upstream and downstream
            - "upstream": Only get upstream dependencies  
            - "downstream": Only get downstream dependencies
        max_depth: Maximum depth to traverse (optional)
        include_issues: Whether to include issue counts for each node (default: True)
        
    Returns:
        Dictionary containing the complete lineage graph
    """
    
    client = get_api_client()
    debug_print(f"Getting lineage graph for node {node_id}, direction: {direction}")
    
    try:
        result = await client.get_lineage_graph(
            node_id=node_id,
            direction=direction,
            max_depth=max_depth,
            include_issues=include_issues
        )
        debug_print("Lineage graph response received")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting lineage graph: {str(e)}"
        }

@mcp.tool()
async def lineage_get_node(
    node_id: int
) -> Dict[str, Any]:
    """Get details for a specific lineage node to verify it exists and check its properties.
    
    This tool retrieves basic information about a lineage node.
    
    Args:
        node_id: The ID of the lineage node to get details for
        
    Returns:
        Dictionary containing the lineage node details
    """
    
    client = get_api_client()
    debug_print(f"Getting details for lineage node {node_id}")
    
    try:
        result = await client.get_lineage_node(node_id=node_id)
        if "nodeType" in result:
            debug_print(f"Found node {node_id}: {result.get('nodeName', 'Unnamed')}")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting lineage node details: {str(e)}"
        }

@mcp.tool()
async def lineage_get_node_issues(
    node_id: int
) -> Dict[str, Any]:
    """Get all data quality issues affecting a specific lineage node.
    
    This tool retrieves all issues that are currently affecting a particular node
    in the data lineage graph.
    
    Args:
        node_id: The ID of the lineage node to get issues for
        
    Returns:
        Dictionary containing all issues for the lineage node
    """
    
    client = get_api_client()
    debug_print(f"Getting issues for lineage node {node_id}")
    
    try:
        result = await client.get_lineage_node_issues(node_id=node_id)
        if "issues" in result:
            issue_count = len(result["issues"])
            debug_print(f"Found {issue_count} issues for node {node_id}")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting lineage node issues: {str(e)}"
        }

@mcp.tool()
async def lineage_analyze_upstream_causes(
    node_id: int,
    max_depth: Optional[int] = 5
) -> Dict[str, Any]:
    """Analyze upstream lineage to identify root causes of data quality issues.
    
    This tool performs upstream root cause analysis by traversing the data lineage
    backwards from a given node.
    
    Args:
        node_id: The ID of the lineage node where issues are occurring
        max_depth: Maximum depth to search upstream (default: 5)
        
    Returns:
        Dictionary containing root cause analysis
    """
    
    debug_print(f"Analyzing upstream root causes for node {node_id}")
    
    try:
        # Get upstream lineage graph
        upstream_result = await lineage_get_graph(
            node_id=node_id,
            direction="upstream",
            max_depth=max_depth,
            include_issues=True
        )
        
        if "error" in upstream_result:
            return upstream_result
        
        debug_print("Processing upstream lineage for root cause analysis")
        
        nodes = upstream_result.get("nodes", {})
        root_causes = []
        issue_path = []
        
        # Analyze nodes to identify root causes
        for node_data in nodes.values():
            lineage_node = node_data.get("lineageNode", {})
            issue_count = node_data.get("issueCount", 0)
            upstream_edges = node_data.get("upstreamEdges", [])
            
            if issue_count > 0:
                has_upstream_issues = False
                
                # Check if upstream nodes have issues
                for edge in upstream_edges:
                    upstream_node_id = edge.get("upstreamNodeId")
                    if str(upstream_node_id) in nodes:
                        upstream_node = nodes[str(upstream_node_id)]
                        if upstream_node.get("issueCount", 0) > 0:
                            has_upstream_issues = True
                            break
                
                if not has_upstream_issues:
                    root_causes.append({
                        "node_id": lineage_node.get("id"),
                        "node_name": lineage_node.get("nodeName"),
                        "node_type": lineage_node.get("nodeType"),
                        "issue_count": issue_count,
                        "catalog_path": lineage_node.get("catalogPath", {}),
                        "is_root_cause": True
                    })
                
                issue_path.append({
                    "node_id": lineage_node.get("id"),
                    "node_name": lineage_node.get("nodeName"),
                    "issue_count": issue_count,
                    "depth": len(upstream_edges)
                })
        
        debug_print(f"Identified {len(root_causes)} potential root causes")
        
        # Generate recommendations
        recommendations = []
        if root_causes:
            recommendations.append("Focus remediation efforts on the identified root cause nodes")
            recommendations.append("Verify data quality in upstream source systems")
        else:
            recommendations.append("No clear root causes found - issues may be at the maximum search depth")
            recommendations.append("Consider increasing max_depth or checking data sources outside the lineage graph")
        
        return {
            "analysis_summary": {
                "target_node_id": node_id,
                "max_depth_searched": max_depth,
                "total_upstream_nodes": len(nodes),
                "nodes_with_issues": len([n for n in nodes.values() if n.get("issueCount", 0) > 0]),
                "root_causes_identified": len(root_causes)
            },
            "root_causes": root_causes,
            "issue_propagation_path": sorted(issue_path, key=lambda x: x["depth"]),
            "upstream_lineage_graph": upstream_result,
            "recommendations": recommendations
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error analyzing upstream root causes: {str(e)}"
        }

@mcp.tool()
async def lineage_analyze_downstream_impact(
    node_id: int,
    max_depth: Optional[int] = 5,
    include_integration_entities: bool = True,
    impact_focus: Optional[str] = "all"
) -> Dict[str, Any]:
    """Analyze downstream impact of data quality issues.
    
    This tool performs downstream impact analysis to understand how data quality issues
    in a given node affect downstream consumers. Can focus on specific types of impact.
    
    Args:
        node_id: The ID of the lineage node with potential data quality issues
        max_depth: Maximum depth to search downstream (default: 5)
        include_integration_entities: Include BI tools, dashboards, etc. (default: True)
        impact_focus: Type of impact to focus on (default: "all"):
            - "all": Show all downstream impacts
            - "analytics": Only BI/reporting tools (Tableau, PowerBI, Looker, etc.)
            - "data_products": Final tables/views that are likely data products
            - "critical": Only nodes with existing issues or high metric counts
        
    Returns:
        Dictionary containing impact analysis with categorized impacts
    """
    
    debug_print(f"Analyzing downstream impact for node {node_id} with focus: {impact_focus}")
    
    try:
        # Get downstream lineage graph
        downstream_result = await lineage_get_graph(
            node_id=node_id,
            direction="downstream",
            max_depth=max_depth,
            include_issues=True
        )
        
        if "error" in downstream_result:
            return downstream_result
        
        debug_print("Processing downstream lineage for impact analysis")
        
        nodes = downstream_result.get("nodes", {})
        impacted_nodes = []
        critical_impacts = []
        integration_entities = []
        analytics_tools = []
        data_products = []
        
        # Known analytics/BI tool types and sources
        analytics_node_types = ["BI_WORKBOOK", "BI_REPORT", "BI_DASHBOARD", "APPLICATION"]
        analytics_sources = ["TABLEAU", "POWERBI", "LOOKER", "QLIK", "SISENSE", "METABASE", "SUPERSET"]
        
        # Analyze downstream nodes for impact
        for node_data in nodes.values():
            lineage_node = node_data.get("lineageNode", {})
            node_type = lineage_node.get("nodeType", "")
            node_name = lineage_node.get("nodeName", "")
            metric_count = node_data.get("metricCount", 0)
            issue_count = node_data.get("issueCount", 0)
            source_name = lineage_node.get("source", {}).get("name", "").upper()
            catalog_path = lineage_node.get("catalogPath", {})
            path_parts = catalog_path.get("pathParts", [])
            
            # Build full qualified name
            full_qualified_name = ".".join(path_parts) if path_parts else node_name
            
            node_info = {
                "node_id": lineage_node.get("id"),
                "node_name": node_name,
                "full_qualified_name": full_qualified_name,
                "USE_THIS_NAME": full_qualified_name,
                "node_type": node_type,
                "metric_count": metric_count,
                "existing_issues": issue_count,
                "catalog_path": catalog_path,
                "source": lineage_node.get("source", {}),
                "source_name": source_name
            }
            
            # Categorize the node
            is_analytics = (node_type in analytics_node_types or 
                          any(tool in source_name for tool in analytics_sources))
            
            # Check if it's a likely data product (endpoint with no downstream)
            edges = downstream_result.get("edges", [])
            has_downstream = any(e.get("fromId") == lineage_node.get("id") for e in edges)
            is_likely_data_product = (not has_downstream and 
                                     node_type == "DATA_NODE_TYPE_TABLE" and
                                     ("PROD" in node_name.upper() or "DIM_" in node_name.upper() or 
                                      "FACT_" in node_name.upper() or "AGG_" in node_name.upper()))
            
            # Categorize impacted nodes
            if is_analytics:
                analytics_tools.append(node_info)
                if include_integration_entities:
                    integration_entities.append(node_info)
            
            if is_likely_data_product:
                data_products.append(node_info)
            
            if metric_count > 0 or issue_count > 0:
                critical_impacts.append(node_info)
            
            # Apply focus filter
            include_node = False
            if impact_focus == "all":
                include_node = True
            elif impact_focus == "analytics" and is_analytics:
                include_node = True
            elif impact_focus == "data_products" and is_likely_data_product:
                include_node = True
            elif impact_focus == "critical" and (metric_count > 0 or issue_count > 0):
                include_node = True
            
            if include_node:
                impacted_nodes.append(node_info)
        
        debug_print(f"Found {len(impacted_nodes)} impacted nodes")
        
        # Assess impact severity
        severity_score = 0
        severity_factors = []
        
        if len(impacted_nodes) > 10:
            severity_score += 2
            severity_factors.append("High number of impacted downstream nodes")
        
        if len(analytics_tools) > 0:
            severity_score += 2
            severity_factors.append(f"Business intelligence tools and reports affected ({len(analytics_tools)} items)")
        
        if len(data_products) > 0:
            severity_score += 1
            severity_factors.append(f"Production data products impacted ({len(data_products)} tables)")
        
        if len(critical_impacts) > 3:
            severity_score += 1
            severity_factors.append("Multiple downstream systems with existing metrics/issues")
        
        # Determine severity level
        severity_level = "HIGH" if severity_score >= 4 else "MEDIUM" if severity_score >= 2 else "LOW"
        
        # Generate stakeholder notifications
        notifications = []
        if analytics_tools:
            bi_tools = set(entity.get("source_name", entity.get("source", {}).get("name", "Unknown")) 
                          for entity in analytics_tools)
            notifications.append(f"Notify BI teams - affected tools: {', '.join(bi_tools)}")
            
            # Add specific analytics tool counts
            tool_counts = {}
            for tool in analytics_tools:
                tool_type = tool.get("source_name", "Unknown")
                tool_counts[tool_type] = tool_counts.get(tool_type, 0) + 1
            
            for tool_type, count in tool_counts.items():
                if tool_type != "Unknown":
                    notifications.append(f"  - {count} {tool_type} dashboard(s)/report(s) affected")
        
        if critical_impacts:
            notifications.append("Alert data engineering teams about downstream data quality impacts")
        
        if data_products:
            notifications.append(f"Production data products affected: {len(data_products)} tables/views")
        
        # Create analytics-specific summary if focus is on analytics
        analytics_summary = None
        if impact_focus == "analytics" and analytics_tools:
            analytics_summary = {
                "total_analytics_nodes": len(analytics_tools),
                "by_tool": {},
                "affected_dashboards": []
            }
            for tool in analytics_tools:
                tool_type = tool.get("source_name", "Unknown")
                if tool_type not in analytics_summary["by_tool"]:
                    analytics_summary["by_tool"][tool_type] = []
                analytics_summary["by_tool"][tool_type].append({
                    "name": tool.get("full_qualified_name"),
                    "id": tool.get("node_id")
                })
                analytics_summary["affected_dashboards"].append(tool.get("full_qualified_name"))
        
        return {
            "impact_summary": {
                "source_node_id": node_id,
                "impact_focus": impact_focus,
                "max_depth_analyzed": max_depth,
                "total_impacted_nodes": len(impacted_nodes),
                "critical_impacts_count": len(critical_impacts),
                "analytics_tools_count": len(analytics_tools),
                "data_products_count": len(data_products),
                "integration_entities_count": len(integration_entities),
                "severity_level": severity_level,
                "severity_score": severity_score
            },
            "impacted_nodes": impacted_nodes,
            "categorized_impacts": {
                "critical": critical_impacts,
                "analytics_tools": analytics_tools,
                "data_products": data_products,
                "integration_entities": integration_entities if include_integration_entities else []
            },
            "analytics_summary": analytics_summary,
            "impact_severity": {
                "level": severity_level,
                "score": severity_score,
                "factors": severity_factors
            },
            "stakeholder_notifications": notifications,
            "downstream_lineage_graph": downstream_result if impact_focus == "all" else None
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error analyzing downstream impact: {str(e)}"
        }

@mcp.tool()
async def lineage_trace_issue_path(
    issue_id: int,
    include_root_cause_analysis: bool = True,
    include_impact_analysis: bool = True,
    max_depth: Optional[int] = 5
) -> Dict[str, Any]:
    """Trace the complete lineage path for a data quality issue from root cause to downstream impact.
    
    This tool provides a comprehensive analysis by combining issue details with lineage tracing.
    
    Args:
        issue_id: The ID of the issue to trace through lineage
        include_root_cause_analysis: Whether to perform upstream root cause analysis (default: True)
        include_impact_analysis: Whether to perform downstream impact analysis (default: True) 
        max_depth: Maximum depth for lineage traversal (default: 5)
        
    Returns:
        Dictionary containing comprehensive lineage analysis
    """
    
    debug_print(f"Tracing lineage path for issue {issue_id}")
    
    try:
        # First get issues to find the specific one
        issues_response = await get_issues(page_size=1000)
        
        if "error" in issues_response:
            return issues_response
        
        # Find the specific issue
        target_issue = None
        for issue in issues_response.get("issues", []):
            if issue.get("id") == issue_id:
                target_issue = issue
                break
        
        if not target_issue:
            return {
                "error": True,
                "message": f"Issue {issue_id} not found"
            }
        
        debug_print(f"Found issue: {target_issue.get('name', 'Unnamed')}")
        
        # Get dataset/table information
        dataset_id = target_issue.get("datasetId")
        if not dataset_id:
            return {
                "error": True,
                "message": f"Unable to determine dataset/lineage node for issue {issue_id}"
            }
        
        lineage_node_id = dataset_id
        
        result = {
            "issue_details": target_issue,
            "lineage_node_id": lineage_node_id,
            "analysis_summary": {
                "issue_id": issue_id,
                "issue_name": target_issue.get("name"),
                "table_name": target_issue.get("tableName"),
                "issue_status": target_issue.get("currentStatus"),
                "issue_priority": target_issue.get("priority"),
                "max_depth_analyzed": max_depth
            }
        }
        
        # Perform root cause analysis if requested
        if include_root_cause_analysis:
            debug_print("Performing root cause analysis")
            root_cause_result = await analyze_upstream_root_causes(
                node_id=lineage_node_id,
                max_depth=max_depth
            )
            result["root_cause_analysis"] = root_cause_result
        
        # Perform impact analysis if requested  
        if include_impact_analysis:
            debug_print("Performing impact analysis")
            impact_result = await analyze_downstream_impact(
                node_id=lineage_node_id,
                max_depth=max_depth,
                include_integration_entities=True
            )
            result["impact_analysis"] = impact_result
        
        # Get the complete bidirectional lineage graph
        debug_print("Getting complete lineage graph")
        full_graph = await lineage_get_graph(
            node_id=lineage_node_id,
            direction="bidirectional",
            max_depth=max_depth,
            include_issues=True
        )
        result["full_lineage_graph"] = full_graph
        
        # Generate remediation plan
        remediation_steps = []
        
        if include_root_cause_analysis and "root_cause_analysis" in result:
            root_causes = result["root_cause_analysis"].get("root_causes", [])
            if root_causes:
                remediation_steps.append("Address root causes in upstream data sources:")
                for rc in root_causes[:3]:
                    remediation_steps.append(f"  - Investigate {rc.get('node_name', 'Unknown')}")
        
        remediation_steps.append(f"Directly address the issue: {target_issue.get('name', 'Unnamed')}")
        
        if include_impact_analysis and "impact_analysis" in result:
            impact_summary = result["impact_analysis"].get("impact_summary", {})
            if impact_summary.get("severity_level", "UNKNOWN") == "HIGH":
                remediation_steps.append("HIGH PRIORITY: Implement immediate monitoring")
        
        result["remediation_plan"] = remediation_steps
        
        debug_print(f"Lineage trace completed for issue {issue_id}")
        return result
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error tracing issue lineage path: {str(e)}"
        }

# Prompts

@mcp.prompt()
def check_connection_info() -> str:
    """Check the connection to Bigeye API."""
    return """
    The Bigeye MCP server is pre-configured with credentials from your Claude Desktop configuration.
    
    To verify the connection:
    ```python
    # Check current status
    auth_status = await read_resource("bigeye://auth/status")
    print(auth_status)
    
    # Check health
    health = await check_health()
    print(f"API Health: {health}")
    
    # Get issues to verify access
    issues = await get_issues(page_size=5)
    print(f"Found {len(issues.get('issues', []))} issues")
    ```
    
    All credentials are managed via your Claude Desktop config file.
    No manual authentication is needed.
    """

@mcp.prompt()
def merge_issues_example() -> str:
    """Example of merging issues using the merge_issues tool."""
    return """
    # Merging Issues into a Single Incident
    
    The Bigeye API allows you to merge multiple related issues into a single incident.
    
    ## Example: Finding and Merging Issues
    
    ```python
    # First, find issues to merge
    issues_response = await get_issues(
        schema_names=["ORDERS"],
        statuses=["ISSUE_STATUS_NEW"]
    )
    
    # Extract issue IDs
    issue_ids = []
    if "issues" in issues_response:
        issue_ids = [issue["id"] for issue in issues_response["issues"][:3]]
        print(f"Found {len(issue_ids)} issues to merge: {issue_ids}")
    
    # Merge the issues if we found at least 2
    if len(issue_ids) >= 2:
        merge_result = await merge_issues(
            issue_ids=issue_ids,
            incident_name="Order data quality issues"
        )
        
        if "incident" in merge_result:
            incident = merge_result["incident"]
            print(f"Created incident with ID: {incident['id']}")
    ```
    """

@mcp.prompt()
def lineage_analysis_examples() -> str:
    """Comprehensive examples of using lineage analysis tools."""
    return """
    # Data Lineage Analysis for Root Cause and Impact Assessment
    
    ## Example: Complete Data Quality Investigation Workflow
    
    ```python
    # Scenario: You have a data quality issue and need to understand its full impact
    issue_id = 12345
    
    # 1. Start with a complete lineage trace for the issue
    full_analysis = await trace_issue_lineage_path(
        issue_id=issue_id,
        include_root_cause_analysis=True,
        include_impact_analysis=True,
        max_depth=7
    )
    
    # Examine the analysis summary
    summary = full_analysis["analysis_summary"]
    print(f"Analyzing issue: {summary['issue_name']}")
    print(f"Status: {summary['issue_status']}, Priority: {summary['issue_priority']}")
    
    # Review root causes
    if "root_cause_analysis" in full_analysis:
        root_causes = full_analysis["root_cause_analysis"]["root_causes"]
        print(f"Found {len(root_causes)} root causes")
    
    # Review downstream impact
    if "impact_analysis" in full_analysis:
        impact = full_analysis["impact_analysis"]["impact_summary"]
        print(f"Impact level: {impact['severity_level']}")
    ```
    
    ## Example: Focused Root Cause Analysis
    
    ```python
    # When you need to focus specifically on finding the source of issues
    node_id = 67890  # Table with data quality problems
    
    root_cause_analysis = await analyze_upstream_root_causes(
        node_id=node_id,
        max_depth=10
    )
    
    # Examine the results
    summary = root_cause_analysis["analysis_summary"]
    print(f"Analyzed {summary['total_upstream_nodes']} upstream nodes")
    print(f"Identified {summary['root_causes_identified']} root causes")
    ```
    """

# ========================================
# Agent Lineage Tracking Tools
# ========================================

@mcp.tool()
async def lineage_track_data_access(
    qualified_names: List[str],
    agent_name: Optional[str] = None
) -> Dict[str, Any]:
    """Track data assets accessed by an AI agent.
    
    This tool allows AI agents to track which tables and columns they've accessed
    during their analysis. The tracked assets can later be committed to Bigeye's
    lineage graph to show data dependencies.
    
    Args:
        qualified_names: List of fully qualified names of accessed assets.
                        Supports formats:
                        - database.schema.table
                        - database.schema.table.column
                        - warehouse.database.schema.table
                        - warehouse.database.schema.table.column
        agent_name: Optional custom name for the agent (defaults to system-based name)
        
    Returns:
        Dictionary containing tracking status and summary
        
    Example:
        # Track table access
        await track_data_access([
            "SNOWFLAKE.SALES.PUBLIC.ORDERS",
            "SNOWFLAKE.SALES.PUBLIC.CUSTOMERS"
        ])
        
        # Track column-level access
        await track_data_access([
            "SALES.PUBLIC.ORDERS.order_id",
            "SALES.PUBLIC.ORDERS.customer_id",
            "SALES.PUBLIC.CUSTOMERS.customer_name"
        ])
    """
    
    if not lineage_tracker:
        return {
            'error': 'Lineage tracker not initialized',
            'hint': 'Authentication may have failed'
        }
    
    try:
        # Update agent name if provided
        if agent_name:
            lineage_tracker.agent_name = agent_name
            
        # Track the assets
        lineage_tracker.track_asset_access(qualified_names)
        
        # Get current tracking status
        tracked = lineage_tracker.get_tracked_assets()
        
        return {
            "success": True,
            "agent_name": lineage_tracker.agent_name,
            "assets_tracked": tracked,
            "message": f"Tracked {tracked['total_tables']} tables and {tracked['total_columns']} columns"
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error tracking data access: {str(e)}"
        }

@mcp.tool()
async def lineage_get_tracking_status() -> Dict[str, Any]:
    """Get the current status of lineage tracking.
    
    Returns information about all data assets currently being tracked
    by the agent, before they are committed to Bigeye's lineage graph.
    
    Returns:
        Dictionary containing tracking status and tracked assets
    """
    
    if not lineage_tracker:
        return {
            'error': 'Lineage tracker not initialized',
            'hint': 'Authentication may have failed'
        }
    
    try:
        tracked = lineage_tracker.get_tracked_assets()
        
        return {
            "success": True,
            "agent_name": lineage_tracker.agent_name,
            "tracked_assets": tracked,
            "ready_to_commit": tracked["total_tables"] > 0
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting tracking status: {str(e)}"
        }

@mcp.tool()
async def lineage_commit_agent(
    rebuild_graph: bool = True,
    clear_after_commit: bool = True
) -> Dict[str, Any]:
    """Commit tracked data access to Bigeye's lineage graph.
    
    This creates lineage edges between the AI agent and all tracked data assets,
    showing which tables and columns the agent has accessed during its analysis.
    
    Args:
        rebuild_graph: Whether to rebuild the lineage graph after creating edges (default: True)
        clear_after_commit: Whether to clear tracked assets after successful commit (default: True)
        
    Returns:
        Dictionary containing commit results and any errors
        
    Example:
        # First track some data access
        await track_data_access([
            "SALES.PUBLIC.ORDERS",
            "SALES.PUBLIC.CUSTOMERS"
        ])
        
        # Then commit to Bigeye
        result = await commit_agent_lineage()
        print(f"Created {result['edges_created']} lineage edges")
    """
    
    if not lineage_tracker:
        return {
            'error': 'Lineage tracker not initialized',
            'hint': 'Authentication may have failed'
        }
    
    try:
        # Create lineage edges
        result = await lineage_tracker.create_lineage_edges(rebuild_graph=rebuild_graph)
        
        # Clear tracked assets if requested and commit was successful
        if clear_after_commit and result.get("success", False):
            lineage_tracker.clear_tracked_assets()
            result["assets_cleared"] = True
            
        return result
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error committing lineage: {str(e)}"
        }

@mcp.tool()
async def lineage_clear_tracked_assets() -> Dict[str, Any]:
    """Clear all tracked data assets without committing.
    
    Use this to reset the tracking state without creating lineage edges.
    
    Returns:
        Dictionary confirming the clear operation
    """
    
    if not lineage_tracker:
        return {
            'error': 'Lineage tracker not initialized',
            'hint': 'Authentication may have failed'
        }
    
    try:
        # Get count before clearing
        tracked = lineage_tracker.get_tracked_assets()
        tables_cleared = tracked["total_tables"]
        
        # Clear
        lineage_tracker.clear_tracked_assets()
        
        return {
            "success": True,
            "message": f"Cleared {tables_cleared} tracked tables",
            "agent_name": lineage_tracker.agent_name
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error clearing tracked assets: {str(e)}"
        }

@mcp.tool()
async def lineage_cleanup_agent_edges(
    retention_days: int = 30
) -> Dict[str, Any]:
    """Clean up old lineage edges for the AI agent.
    
    This removes lineage edges older than the specified retention period,
    but ONLY for edges where the AI agent is involved. This ensures we
    don't accidentally delete existing lineage between tables.
    
    Args:
        retention_days: Number of days to retain lineage edges (default: 30)
        
    Returns:
        Dictionary containing cleanup results
        
    Example:
        # Clean up edges older than 7 days
        result = await cleanup_agent_lineage_edges(retention_days=7)
        print(f"Deleted {result['edges_deleted']} old edges")
    """
    
    if not lineage_tracker:
        return {
            'error': 'Lineage tracker not initialized',
            'hint': 'Authentication may have failed'
        }
    
    try:
        result = await lineage_tracker.cleanup_old_edges(retention_days=retention_days)
        return result
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error cleaning up lineage edges: {str(e)}"
        }

@mcp.tool()
async def lineage_find_node(
    workspace_id: Optional[int] = None,
    search_string: str = "*",
    node_type: Optional[str] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """Find lineage nodes and get their IDs using Bigeye's advanced search.
    
    This tool uses Bigeye's path-based search to find nodes in the lineage graph.
    It's particularly useful for getting node IDs that can be used with other lineage tools.
    
    Search format supports:
    - Path-based search: "warehouse/schema/table/column"
    - Partial names: Search for any part of the hierarchy
    
    Args:
        workspace_id: Optional Bigeye workspace ID. If not provided, uses the configured workspace.
        search_string: Search string using path format or partial names\
        node_type: Optional node type filter:
            - "DATA_NODE_TYPE_TABLE" - Tables only
            - "DATA_NODE_TYPE_COLUMN" - Columns only
            - "DATA_NODE_TYPE_CUSTOM" - Custom nodes (e.g., AI agents)
        limit: Maximum number of results to return (default: 20)
        
    Returns:
        Dictionary containing found nodes with their IDs and details
        
    Examples:
        # Find a specific table (uses configured workspace)
        await lineage_find_node(search_string="SNOWFLAKE/PROD_REPL/DIM_CUSTOMER")
        
        # Find all tables with "CUSTOMER" in the name
        await lineage_find_node(search_string="*CUSTOMER*")
        
        # Find only table nodes with "CUSTOMER" 
        await lineage_find_node(search_string="*CUSTOMER*", node_type="DATA_NODE_TYPE_TABLE")
        
        # Find a specific column
        await lineage_find_node(search_string="SNOWFLAKE/PROD_REPL/DIM_CUSTOMER/CUSTOMER_ID")
        
        # Find all custom nodes (AI agents)
        await lineage_find_node(search_string="*", node_type="DATA_NODE_TYPE_CUSTOM")
        
        # Find custom nodes with "Claude" in the name
        await lineage_find_node(search_string="Claude", node_type="DATA_NODE_TYPE_CUSTOM")
    """
    # Use configured workspace_id if not provided
    if workspace_id is None:
        workspace_id = config.get('workspace_id')
        if not workspace_id:
            return {
                'error': 'Workspace ID not configured',
                'hint': 'Check your Claude Desktop configuration for BIGEYE_WORKSPACE_ID'
            }
    
    # Enhanced debug logging
    debug_print(f"=== lineage_find_node called ===")
    debug_print(f"  workspace_id: {workspace_id} (type: {type(workspace_id)})")
    debug_print(f"  search_string: '{search_string}'")
    debug_print(f"  node_type: {node_type}")
    debug_print(f"  limit: {limit}")
    debug_print(f"  auth_client.is_authenticated: {auth_client.is_authenticated}")
    debug_print(f"  config.get('workspace_id'): {config.get('workspace_id')}")
    
    
    client = get_api_client()
    if not client:
        return {'error': 'Failed to get API client'}
    
    # Ensure workspace_id is an integer
    try:
        workspace_id = int(workspace_id)
        debug_print(f"Converted workspace_id to int: {workspace_id}")
    except (ValueError, TypeError) as e:
        error_msg = f"workspace_id must be a valid integer, got: {workspace_id} (type: {type(workspace_id)})"
        debug_print(f"ERROR: {error_msg}")
        return {
            "error": True,
            "message": error_msg
        }
        
    try:
        # Normalize the search string (trim whitespace around slashes)
        normalized_search = search_string.strip().replace(' / ', '/').replace('/ ', '/').replace(' /', '/')
        
        debug_print(f"Normalized search string: '{normalized_search}'")
        debug_print(f"Calling client.search_lineage_v2 with:")
        debug_print(f"  search_string: '{normalized_search}'")
        debug_print(f"  workspace_id: {workspace_id}")
        debug_print(f"  limit: {limit}")
        
        # Use the v2 search API
        result = await client.search_lineage_v2(
            search_string=normalized_search,
            workspace_id=workspace_id,
            limit=limit
        )
        
        debug_print(f"API response error status: {result.get('error')}")
        debug_print(f"Full API response: {result}")
        
        if result.get("error"):
            debug_print(f"Returning error response: {result}")
            return result
            
        # Extract and format results
        nodes = result.get("results", [])
        debug_print(f"Found {len(nodes)} nodes in results")
        
        formatted_nodes = []
        
        for node in nodes:
            # Filter by node type if specified
            if node_type and node.get("nodeType") != node_type:
                debug_print(f"Skipping node due to type mismatch: {node.get('nodeType')} != {node_type}")
                continue
                
            # Build the display path
            catalog_path = node.get("catalogPath", {})
            path_parts = catalog_path.get("pathParts", [])
            
            # Format the path for display
            if path_parts:
                display_path = " / ".join(path_parts)
            else:
                display_path = node.get("nodeName", "Unknown")
            
            # Build full qualified name from path parts
            full_qualified_name = display_path.replace(" / ", ".")
            
            formatted_node = {
                "id": node.get("id"),
                "full_qualified_name": full_qualified_name,
                "USE_THIS_NAME": full_qualified_name,
                "display_name": f"{full_qualified_name} ({node.get('nodeType', 'Unknown type')})",
                "name": node.get("nodeName"),
                "type": node.get("nodeType"),
                "path": display_path,
                "container": node.get("nodeContainerName"),
                "catalog_path": catalog_path
            }
            formatted_nodes.append(formatted_node)
            debug_print(f"Added node: {formatted_node}")
        
        debug_print(f"Returning {len(formatted_nodes)} formatted nodes")
        
        return {
            "search_string": search_string,
            "normalized_search": normalized_search,
            "node_type_filter": node_type,
            "found_count": len(formatted_nodes),
            "nodes": formatted_nodes,
            "hint": "Use the 'id' field from results with other lineage tools"
        }
        
    except Exception as e:
        error_msg = f"Search failed: {str(e)}"
        debug_print(f"ERROR in lineage_find_node: {error_msg}")
        import traceback
        debug_print(f"Traceback: {traceback.format_exc()}")
        return {
            "error": True,
            "message": error_msg
        }

@mcp.tool()
async def lineage_explore_catalog(
    schema_name: Optional[str] = None,
    warehouse_name: Optional[str] = None,
    search_term: Optional[str] = None,
    page_size: int = 50
) -> Dict[str, Any]:
    """Explore tables in Bigeye's catalog.
    
    This diagnostic tool helps discover how tables are named and structured in Bigeye's catalog.
    
    Args:
        schema_name: Optional schema name to filter by (e.g., "PROD_REPL")
        warehouse_name: Optional warehouse name to filter by (e.g., "SNOWFLAKE")
        search_term: Optional search term to filter table names
        page_size: Number of results to return (default: 50)
        
    Returns:
        Dictionary containing catalog tables with their full names
        
    Example:
        # Find all tables in PROD_REPL schema
        await explore_catalog_tables(schema_name="PROD_REPL")
        
        # Find tables with "ORDER" in the name
        await explore_catalog_tables(search_term="ORDER")
    """
    
    client = get_api_client()
    if not client:
        return {'error': 'Failed to get API client'}
        
    try:
        # Get tables from catalog
        result = await client.get_catalog_tables(
            workspace_id=config.get('workspace_id'),
            schema_name=schema_name,
            warehouse_name=warehouse_name,
            page_size=page_size
        )
        
        if result.get("error"):
            return result
            
        tables = result.get("tables", [])
        
        # Filter by search term if provided
        if search_term:
            search_upper = search_term.upper()
            tables = [t for t in tables if search_upper in t.get("tableName", "").upper()]
        
        # Format the results with emphasized full qualified names
        formatted_tables = []
        for table in tables:
            warehouse = table.get('warehouseName', '')
            schema = table.get('schemaName', '')
            table_name = table.get('tableName', '')
            
            full_qualified_name = ""
            if warehouse and schema and table_name:
                full_qualified_name = f"{warehouse}.{schema}.{table_name}"
            elif schema and table_name:
                full_qualified_name = f"{schema}.{table_name}"
            elif table_name:
                full_qualified_name = table_name
                
            formatted_tables.append({
                "full_qualified_name": full_qualified_name,
                "USE_THIS_NAME": full_qualified_name,
                "display_name": f"{full_qualified_name} (in {warehouse} database)" if warehouse else full_qualified_name,
                "id": table.get("id"),
                "name": table_name,
                "schema": schema,
                "warehouse": warehouse,
                "catalog_path": table.get("catalogPath")
            })
            
        return {
            "schema_filter": schema_name,
            "warehouse_filter": warehouse_name,
            "search_term": search_term,
            "found_count": len(formatted_tables),
            "tables": formatted_tables[:20],  # Limit to first 20 for readability
            "note": f"Showing first {min(20, len(formatted_tables))} of {len(formatted_tables)} tables"
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Catalog exploration failed: {str(e)}"
        }


@mcp.tool()
async def lineage_delete_node(
    node_id: int,
    force: bool = False
) -> Dict[str, Any]:
    """Delete a custom lineage node from Bigeye's lineage graph.
    
    This tool removes a custom lineage node (such as an AI agent node) from 
    the lineage graph. It will also attempt to remove all associated edges.
    
    WARNING: This operation cannot be undone. Use with caution.
    
    Args:
        node_id: The ID of the custom lineage node to delete
        force: Force deletion even if node has active edges (default: False)
        
    Returns:
        Dictionary containing deletion results
        
    Example:
        # Delete an agent node
        result = await lineage_delete_node(node_id=12345)
        print(f"Deleted node {result['node_id']}")
        
        # Force delete even with edges
        result = await lineage_delete_node(node_id=12345, force=True)
    """
    
    client = get_api_client()
    if not client:
        return {'error': 'Failed to get API client'}
    
    try:
        # First, get the node details to confirm it exists and is custom
        node_result = await client.get_lineage_node(node_id=node_id)
        
        if node_result.get("error"):
            return {
                "error": True,
                "message": f"Cannot find node {node_id}: {node_result.get('message', 'Unknown error')}"
            }
        
        node_type = node_result.get("nodeType", "")
        node_name = node_result.get("nodeName", "Unknown")
        
        # Safety check: only allow deletion of custom nodes
        if node_type != "DATA_NODE_TYPE_CUSTOM":
            return {
                "error": True,
                "message": f"Cannot delete node {node_id}: Only custom nodes can be deleted. This node is type: {node_type}"
            }
        
        # If not forcing, check for edges
        if not force:
            # Try to get edges for this node
            edges_result = await client.get_lineage_edges_for_node(node_id=node_id)
            
            if not edges_result.get("error"):
                edges = edges_result.get("edges", [])
                if edges:
                    return {
                        "error": True,
                        "message": f"Node {node_id} has {len(edges)} active edges. Use force=True to delete anyway.",
                        "node_name": node_name,
                        "edge_count": len(edges)
                    }
        
        # Proceed with deletion
        delete_result = await client.delete_lineage_node(node_id=node_id, force=force)
        
        if delete_result.get("error"):
            return {
                "error": True,
                "message": f"Failed to delete node {node_id}: {delete_result.get('message', 'Unknown error')}"
            }
        
        return {
            "success": True,
            "message": f"Successfully deleted custom lineage node",
            "node_id": node_id,
            "node_name": node_name,
            "node_type": node_type
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error deleting lineage node: {str(e)}"
        }

@mcp.tool()
async def search_schemas(
    schema_name: Optional[str] = None,
    warehouse_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Search for schemas in Bigeye.
    
    This tool searches for database schemas by name and/or warehouse.
    
    Args:
        schema_name: Optional schema name to search for (supports partial matching)
        warehouse_names: Optional list of warehouse names to filter by
        
    Returns:
        Dictionary containing matching schemas
        
    Example:
        # Search for all schemas containing "prod"
        results = await search_schemas(schema_name="prod")
        
        # Search for schemas in specific warehouse
        results = await search_schemas(warehouse_names=["SNOWFLAKE"])
    """
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    
    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration for BIGEYE_WORKSPACE_ID'
        }
    
    debug_print(f"Searching for schemas: name='{schema_name}', warehouses={warehouse_names}")
    
    try:
        result = await client.search_schemas(
            workspace_id=workspace_id,
            schema_name=schema_name,
            warehouse_ids=None  # TODO: Convert warehouse names to IDs if needed
        )
        
        if result.get("error"):
            return result
            
        schemas = result.get("schemas", [])
        
        return {
            "total_results": len(schemas),
            "schemas": [
                {
                    "id": schema.get("id"),
                    "name": schema.get("name"),
                    "warehouse": schema.get("warehouseName"),
                    "table_count": schema.get("tableCount", 0)
                }
                for schema in schemas
            ]
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error searching schemas: {str(e)}"
        }

@mcp.tool()
async def search_tables(
    table_name: Optional[str] = None,
    schema_names: Optional[List[str]] = None,
    warehouse_names: Optional[List[str]] = None,
    include_columns: bool = False
) -> Dict[str, Any]:
    """Search for tables in Bigeye.
    
    ALWAYS USE THIS TOOL FIRST when a user asks about a table by name!
    This tool searches for database tables and helps identify the exact table
    the user is referring to. Present the results to the user and ask them to
    confirm which table they meant before using any other table-related tools.
    
    IMPORTANT: Always refer to tables by their FULL QUALIFIED NAME when discussing
    them with the user (e.g., "ORACLE.PROD_SCHEMA.ORDERS" not just "ORDERS").
    This avoids confusion about which database system the table belongs to.
    
    Args:
        table_name: Optional table name to search for (supports partial matching)
        schema_names: Optional list of schema names to filter by
        warehouse_names: Optional list of warehouse names to filter by
        include_columns: Whether to include column information in the response
        
    Returns:
        Dictionary containing matching tables
        
    Example:
        # Search for tables with "orders" in the name
        results = await search_tables(table_name="orders")
        
        # Search for tables in specific schemas
        results = await search_tables(schema_names=["prod_repl", "staging"])
        
        # Get tables with column details
        results = await search_tables(table_name="customers", include_columns=True)
    """
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    
    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration for BIGEYE_WORKSPACE_ID'
        }
    
    debug_print(f"Searching for tables: name='{table_name}', schemas={schema_names}, warehouses={warehouse_names}")
    
    try:
        result = await client.search_tables(
            workspace_id=workspace_id,
            table_name=table_name,
            schema_names=schema_names,
            warehouse_ids=None,  # TODO: Convert warehouse names to IDs if needed
            include_columns=include_columns
        )
        
        if result.get("error"):
            return result
            
        tables = result.get("tables", [])
        
        formatted_tables = []
        for table in tables:
            # Build the full qualified name
            warehouse = table.get("warehouseName", "")
            database = table.get("databaseName", "")
            schema = table.get("schemaName", "")
            name = table.get("name", "")
            
            # Create full qualified name (warehouse.database.schema.table or database.schema.table)
            full_parts = [p for p in [warehouse, database, schema, name] if p]
            full_qualified_name = ".".join(full_parts)
            
            formatted_table = {
                "id": table.get("id"),
                "name": name,
                "schema": schema,
                "database": database,
                "warehouse": warehouse,
                "full_qualified_name": full_qualified_name,
                "display_name": f"{full_qualified_name} (in {warehouse or database} database)",
                "row_count": table.get("rowCount"),
                "last_updated": table.get("lastUpdatedAt"),
                "USE_THIS_NAME": full_qualified_name  # Emphasized field for Claude
            }
            
            if include_columns and table.get("columns"):
                formatted_table["columns"] = [
                    {
                        "id": col.get("id"),
                        "name": col.get("name"),
                        "type": col.get("type"),
                        "nullable": col.get("isNullable", True)
                    }
                    for col in table.get("columns", [])
                ]
                
            formatted_tables.append(formatted_table)
        
        return {
            "total_results": len(formatted_tables),
            "tables": formatted_tables
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error searching tables: {str(e)}"
        }

@mcp.tool()
async def get_upstream_issues_for_report(
    report_id: int
) -> Dict[str, Any]:
    """Get upstream data quality issues for a BI report (like a Tableau workbook).

    This tool retrieves all data quality issues in tables that feed into a specific
    BI report or dashboard, helping identify why reports might have data quality problems.

    Args:
        report_id: The ID of the BI report/dashboard node in the lineage graph

    Returns:
        Dictionary containing upstream issues affecting the report

    Example:
        # Get upstream issues for a Tableau workbook
        issues = await get_upstream_issues_for_report(report_id=12345)
        print(f"Found {len(issues.get('issues', []))} upstream issues")
    """

    client = get_api_client()
    debug_print(f"Getting upstream issues for report {report_id}")

    try:
        result = await client.get_upstream_issues_for_report(report_id=report_id)

        if result.get("error"):
            return result

        # Add summary information
        issues = result.get("issues", [])
        total_issues = len(issues)

        if total_issues > 0:
            # Group issues by status and severity
            status_counts = {}
            severity_counts = {}
            affected_tables = set()

            for issue in issues:
                status = issue.get("currentStatus", "UNKNOWN")
                status_counts[status] = status_counts.get(status, 0) + 1

                priority = issue.get("priority", "UNKNOWN")
                severity_counts[priority] = severity_counts.get(priority, 0) + 1

                # Track affected tables
                table_name = issue.get("tableName")
                schema_name = issue.get("schemaName")
                warehouse_name = issue.get("warehouseName")

                if table_name:
                    full_table = f"{warehouse_name}.{schema_name}.{table_name}" if warehouse_name and schema_name else table_name
                    affected_tables.add(full_table)

            result["summary"] = {
                "total_issues": total_issues,
                "by_status": status_counts,
                "by_priority": severity_counts,
                "affected_tables_count": len(affected_tables),
                "affected_tables": list(affected_tables)[:10]  # Show first 10 tables
            }

            debug_print(f"Found {total_issues} upstream issues for report {report_id}")
        else:
            debug_print(f"No upstream issues found for report {report_id}")
            result["summary"] = {
                "total_issues": 0,
                "message": "No data quality issues found in upstream data sources"
            }

        return result

    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting upstream issues for report: {str(e)}"
        }

@mcp.tool()
async def get_profile_for_table(
    table_id: int
) -> Dict[str, Any]:
    """Get profile report for a table.

    This tool retrieves the data profiling report for a specific table in Bigeye,
    providing insights into data quality characteristics such as:
    - Column-level statistics (nulls, uniqueness, data types)
    - Data distribution patterns
    - Data quality scores
    - Freshness information
    - Profile execution history

    Args:
        table_id: The ID of the table to get the profile for

    Returns:
        Dictionary containing the table's profile report

    Example:
        # Get profile for table with ID 12345
        profile = await get_profile_for_table(table_id=12345)
        print(f"Table has {profile.get('column_count', 0)} columns")
    """

    client = get_api_client()
    debug_print(f"Getting profile for table {table_id}")

    try:
        result = await client.get_profile_for_table(table_id=table_id)

        if result.get("error"):
            return result

        # Add summary information if profile data is available
        if result and not result.get("error"):
            summary = {
                "table_id": table_id,
                "profile_available": True
            }

            # Extract key profile metrics if available
            if "columns" in result:
                summary["column_count"] = len(result["columns"])

            if "rowCount" in result:
                summary["row_count"] = result["rowCount"]

            if "lastProfiledAt" in result:
                summary["last_profiled"] = result["lastProfiledAt"]

            result["summary"] = summary
            debug_print(f"Profile retrieved for table {table_id}")
        else:
            debug_print(f"No profile data found for table {table_id}")
            result["summary"] = {
                "table_id": table_id,
                "profile_available": False,
                "message": "No profile data available for this table"
            }

        return result

    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting profile for table: {str(e)}"
        }

@mcp.tool()
async def queue_table_profile(
    table_id: int
) -> Dict[str, Any]:
    """Queue a profiling job for a table.

    This tool initiates a data profiling workflow for a specific table in Bigeye.
    The profiling process analyzes the table's data to generate quality metrics,
    column statistics, and other profile information.

    Args:
        table_id: The ID of the table to queue profiling for

    Returns:
        Dictionary containing the workflow ID of the queued profiling job

    Example:
        # Queue profiling for table with ID 12345
        result = await queue_table_profile(table_id=12345)
        workflow_id = result.get("workflowId")
    """
    client = get_api_client()
    debug_print(f"Queuing profile job for table {table_id}")

    try:
        result = await client.queue_table_profile(table_id=table_id)
        if result.get("error"):
            return result

        # Add summary information
        if result and not result.get("error"):
            summary = {
                "table_id": table_id,
                "profiling_queued": True,
                "workflow_id": result.get("workflowId")
            }
            result["summary"] = summary
            debug_print(f"Profile job queued for table {table_id}, workflow ID: {result.get('workflowId')}")

        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error queuing profile for table: {str(e)}"
        }

@mcp.tool()
async def get_profile_workflow_status_for_table(
    table_id: int
) -> Dict[str, Any]:
    """Get the status of profiling workflow for a table.

    This tool checks the current status of data profiling workflows for a specific table.
    Use this to monitor the progress of profiling jobs that were previously queued.

    Args:
        table_id: The ID of the table to check profiling workflow status for

    Returns:
        Dictionary containing the workflow status information including:
        - Workflow ID and current status
        - Progress information if available
        - Completion time if finished

    Example:
        # Check profiling status for table with ID 12345
        status = await get_profile_workflow_status_for_table(table_id=12345)
        print(f"Workflow status: {status.get('status')}")
    """
    client = get_api_client()
    debug_print(f"Getting profile workflow status for table {table_id}")

    try:
        result = await client.get_profile_workflow_status_for_table(table_id=table_id)
        if result.get("error"):
            return result

        # Add summary information
        if result and not result.get("error"):
            summary = {
                "table_id": table_id,
                "status_retrieved": True
            }
            # Extract key status information if available
            if "status" in result:
                summary["workflow_status"] = result["status"]
            if "workflowId" in result:
                summary["workflow_id"] = result["workflowId"]
            result["summary"] = summary
            debug_print(f"Profile workflow status retrieved for table {table_id}")

        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting profile workflow status for table: {str(e)}"
        }

@mcp.tool()
async def search_columns(
    column_name: Optional[str] = None,
    table_names: Optional[List[str]] = None,
    schema_names: Optional[List[str]] = None,
    warehouse_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Search for columns in Bigeye.
    
    ALWAYS USE THIS TOOL FIRST when a user asks about a column by name!
    This tool searches for database columns and helps identify the exact column
    the user is referring to. Present the results to the user and ask them to
    confirm which column they meant before using any other column-related tools.
    
    IMPORTANT: Always refer to columns by their FULL QUALIFIED NAME when discussing
    them with the user (e.g., "ORACLE.PROD_SCHEMA.ORDERS.CUSTOMER_ID" not just "CUSTOMER_ID").
    This clearly shows which database system and table the column belongs to.
    
    Args:
        column_name: Optional column name to search for (supports partial matching)
        table_names: Optional list of table names to filter by
        schema_names: Optional list of schema names to filter by
        warehouse_names: Optional list of warehouse names to filter by
        
    Returns:
        Dictionary containing matching columns
        
    Example:
        # Search for columns with "customer_id" in the name
        results = await search_columns(column_name="customer_id")
        
        # Search for columns in specific tables
        results = await search_columns(table_names=["orders", "customers"])
        
        # Search for all columns in a schema
        results = await search_columns(schema_names=["prod_repl"])
    """
    client = get_api_client()
    workspace_id = config.get('workspace_id')
    
    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration for BIGEYE_WORKSPACE_ID'
        }
    
    debug_print(f"Searching for columns: name='{column_name}', tables={table_names}, schemas={schema_names}")
    
    try:
        result = await client.search_columns(
            workspace_id=workspace_id,
            column_name=column_name,
            table_names=table_names,
            schema_names=schema_names,
            warehouse_ids=None  # TODO: Convert warehouse names to IDs if needed
        )
        
        if result.get("error"):
            return result
            
        columns = result.get("columns", [])
        
        formatted_columns = []
        for column in columns:
            # Build the full qualified name
            warehouse = column.get("warehouseName", "")
            database = column.get("databaseName", "")
            schema = column.get("schemaName", "")
            table = column.get("tableName", "")
            name = column.get("name", "")
            
            # Create full qualified name for the column
            full_parts = [p for p in [warehouse, database, schema, table, name] if p]
            full_qualified_name = ".".join(full_parts)
            
            formatted_columns.append({
                "id": column.get("id"),
                "name": name,
                "table": table,
                "schema": schema,
                "database": database,
                "warehouse": warehouse,
                "type": column.get("type"),
                "nullable": column.get("isNullable", True),
                "full_qualified_name": full_qualified_name,
                "display_name": f"{full_qualified_name} (in {warehouse or database} database)",
                "USE_THIS_NAME": full_qualified_name  # Emphasized field for Claude
            })
        
        return {
            "total_results": len(formatted_columns),
            "columns": formatted_columns
        }
        
    except Exception as e:
        return {
            "error": True,
            "message": f"Error searching columns: {str(e)}"
        }

# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()