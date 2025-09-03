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
    
    This server provides tools for interacting with Bigeye's data quality monitoring:
    - Query and manage data quality issues
    - Analyze data lineage and dependencies
    - Track AI agent data access patterns
    - Perform root cause analysis
    
    The server is pre-configured with credentials from environment variables.
    All tools are ready to use immediately.
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
    api_key=config["api_key"]
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

# Tools
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
    """Get issues from the Bigeye API.
    
    Args:
        statuses: Optional list of issue statuses to filter by. Possible values:
            - ISSUE_STATUS_NEW
            - ISSUE_STATUS_ACKNOWLEDGED
            - ISSUE_STATUS_CLOSED
            - ISSUE_STATUS_MONITORING
            - ISSUE_STATUS_MERGED
        schema_names: Optional list of schema names to filter issues by
        page_size: Optional number of issues to return per page
        page_cursor: Cursor for pagination
        
    Returns:
        Dictionary containing the issues
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
        page_size=page_size,
        page_cursor=page_cursor
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
    
    This tool fetches all issues related to a specific table in Bigeye,
    making it easier to check data quality for individual tables.
    
    Args:
        table_name: Name of the table (e.g., "ORDERS")
        warehouse_name: Optional warehouse name (e.g., "SNOWFLAKE")
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
            
        # Get the table details
        table_info = {
            "table_name": matching_table.get("tableName"),
            "schema_name": matching_table.get("schemaName"),
            "warehouse_name": matching_table.get("warehouseName"),
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
    include_integration_entities: bool = True
) -> Dict[str, Any]:
    """Analyze downstream impact of data quality issues.
    
    This tool performs downstream impact analysis to understand how data quality issues
    in a given node affect downstream consumers.
    
    Args:
        node_id: The ID of the lineage node with potential data quality issues
        max_depth: Maximum depth to search downstream (default: 5)
        include_integration_entities: Include BI tools, dashboards, etc. (default: True)
        
    Returns:
        Dictionary containing impact analysis
    """
    
    debug_print(f"Analyzing downstream impact for node {node_id}")
    
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
        
        # Analyze downstream nodes for impact
        for node_data in nodes.values():
            lineage_node = node_data.get("lineageNode", {})
            node_type = lineage_node.get("nodeType", "")
            metric_count = node_data.get("metricCount", 0)
            issue_count = node_data.get("issueCount", 0)
            
            node_info = {
                "node_id": lineage_node.get("id"),
                "node_name": lineage_node.get("nodeName"),
                "node_type": node_type,
                "metric_count": metric_count,
                "existing_issues": issue_count,
                "catalog_path": lineage_node.get("catalogPath", {}),
                "source": lineage_node.get("source", {})
            }
            
            # Categorize impacted nodes
            if node_type in ["BI_WORKBOOK", "BI_REPORT", "APPLICATION"]:
                if include_integration_entities:
                    integration_entities.append(node_info)
            elif metric_count > 0 or issue_count > 0:
                critical_impacts.append(node_info)
            
            impacted_nodes.append(node_info)
        
        debug_print(f"Found {len(impacted_nodes)} impacted nodes")
        
        # Assess impact severity
        severity_score = 0
        severity_factors = []
        
        if len(impacted_nodes) > 10:
            severity_score += 2
            severity_factors.append("High number of impacted downstream nodes")
        
        if len(integration_entities) > 0:
            severity_score += 2
            severity_factors.append("Business intelligence tools and reports affected")
        
        if len(critical_impacts) > 3:
            severity_score += 1
            severity_factors.append("Multiple downstream systems with existing metrics/issues")
        
        # Determine severity level
        severity_level = "HIGH" if severity_score >= 4 else "MEDIUM" if severity_score >= 2 else "LOW"
        
        # Generate stakeholder notifications
        notifications = []
        if integration_entities:
            bi_tools = set(entity.get("source", {}).get("name", "Unknown") for entity in integration_entities)
            notifications.append(f"Notify BI teams - affected tools: {', '.join(bi_tools)}")
        
        if critical_impacts:
            notifications.append("Alert data engineering teams about downstream data quality impacts")
        
        return {
            "impact_summary": {
                "source_node_id": node_id,
                "max_depth_analyzed": max_depth,
                "total_impacted_nodes": len(impacted_nodes),
                "critical_impacts_count": len(critical_impacts),
                "integration_entities_count": len(integration_entities),
                "severity_level": severity_level,
                "severity_score": severity_score
            },
            "impacted_nodes": impacted_nodes,
            "critical_impacts": critical_impacts,
            "integration_entities": integration_entities if include_integration_entities else [],
            "impact_severity": {
                "level": severity_level,
                "score": severity_score,
                "factors": severity_factors
            },
            "stakeholder_notifications": notifications,
            "downstream_lineage_graph": downstream_result
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
    - Wildcards: "*" matches any characters
    - Partial names: Search for any part of the hierarchy
    - Use "*" or empty string to search all nodes
    
    Args:
        workspace_id: Optional Bigeye workspace ID. If not provided, uses the configured workspace.
        search_string: Search string using path format or partial names (default: "*" for all)
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
        
        # Find all tables in a schema
        await lineage_find_node(search_string="SNOWFLAKE/PROD_REPL/*")
        
        # Find all tables with "CUSTOMER" in the name
        await lineage_find_node(search_string="*CUSTOMER*")
        
        # Find only table nodes with "CUSTOMER" 
        await lineage_find_node(search_string="*CUSTOMER*", node_type="DATA_NODE_TYPE_TABLE")
        
        # Find a specific column
        await lineage_find_node(search_string="SNOWFLAKE/PROD_REPL/DIM_CUSTOMER/CUSTOMER_ID")
        
        # Find all custom nodes (AI agents)
        await lineage_find_node(search_string="*", node_type="DATA_NODE_TYPE_CUSTOM")
        
        # Find custom nodes with "Claude" in the name
        await lineage_find_node(search_string="*Claude*", node_type="DATA_NODE_TYPE_CUSTOM")
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
            
            formatted_node = {
                "id": node.get("id"),
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
        
        # Format the results
        formatted_tables = []
        for table in tables:
            formatted_tables.append({
                "id": table.get("id"),
                "name": table.get("tableName"),
                "schema": table.get("schemaName"),
                "warehouse": table.get("warehouseName"),
                "full_name": f"{table.get('warehouseName', '')}.{table.get('schemaName', '')}.{table.get('tableName', '')}",
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

# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()