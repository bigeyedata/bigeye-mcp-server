"""
MCP Server for Bigeye API with Dynamic Authentication

This server connects to the Bigeye Datawatch API and exposes resources and tools
for interacting with data quality monitoring. It supports dynamic authentication
through chat without requiring configuration files.
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

# Create an MCP server
mcp = FastMCP("Bigeye API")

# Debug function
def debug_print(message: str):
    """Print debug messages to stderr"""
    if config["debug"] or os.environ.get("BIGEYE_DEBUG", "false").lower() in ["true", "1", "yes"]:
        print(f"[BIGEYE MCP DEBUG] {message}", file=sys.stderr)

# Initialize clients
auth_client = BigeyeAuthClient()
api_client = None

# Check if we have pre-configured credentials (like main branch)
if config.get("api_key") and config.get("workspace_id"):
    # Use config-based authentication (main branch style)
    debug_print(f"Using pre-configured authentication: {config['api_url']}")
    auth_client.set_credentials(
        config["api_url"],
        config["workspace_id"],
        config["api_key"]
    )
    api_client = BigeyeAPIClient(
        api_url=config["api_url"],
        api_key=config["api_key"]
    )
else:
    # Dynamic authentication mode
    debug_print("Starting in dynamic authentication mode")

def get_api_client() -> BigeyeAPIClient:
    """Get or create API client with current authentication"""
    global api_client
    if not auth_client.is_authenticated:
        return None
    
    if api_client is None or api_client.api_url != auth_client.api_base_url:
        api_client = BigeyeAPIClient(
            api_url=auth_client.current_instance,
            api_key=auth_client.api_key
        )
    return api_client

# Authentication status resource
@mcp.resource("bigeye://auth/status")
async def auth_status() -> str:
    """Current authentication status"""
    if auth_client.is_authenticated:
        return f"""Authenticated to Bigeye:
- Instance: {auth_client.current_instance}
- Workspace ID: {auth_client.current_workspace_id}
- Status: ✓ Connected"""
    else:
        saved = auth_client.storage.list_saved_credentials()
        if saved:
            return f"""Not authenticated. Saved credentials available for:
{json.dumps(saved, indent=2)}

Use 'authenticate_bigeye' tool to connect."""
        else:
            return """Not authenticated to Bigeye.

Use 'authenticate_bigeye' tool with your API key to connect."""

# Main authentication tool
@mcp.tool()
async def authenticate_bigeye(
    api_key: str,
    instance: str = "https://app.bigeye.com",
    workspace_id: Optional[int] = None,
    save_credentials: bool = True
) -> Dict[str, Any]:
    """
    Authenticate with Bigeye using an API key.
    
    If workspace_id is not provided, will return a list of available workspaces.
    
    Args:
        api_key: Your Bigeye API key
        instance: Bigeye instance URL (e.g., "demo.bigeye.com", "app.bigeye.com", or full URL)
        workspace_id: Specific workspace to connect to (optional)
        save_credentials: Whether to save credentials for future use
    """
    # Normalize instance URL
    instance = instance.rstrip('/').lower()
    
    # Handle common instance names
    if instance in ['demo', 'demo.bigeye.com']:
        instance = 'https://demo.bigeye.com'
    elif instance in ['app', 'app.bigeye.com', 'prod', 'production']:
        instance = 'https://app.bigeye.com'
    elif instance == 'staging' or instance == 'staging.bigeye.com':
        instance = 'https://staging.bigeye.com'
    elif not instance.startswith('http://') and not instance.startswith('https://'):
        # Add https:// to any other domain-like input
        instance = f'https://{instance}'
    
    # Test authentication
    try:
        auth_result = await auth_client.test_authentication(instance, api_key)
        
        # Ensure auth_result is a dictionary
        if not isinstance(auth_result, dict):
            return {
                'success': False,
                'error': f'Invalid authentication response type: {type(auth_result)}',
                'hint': 'Internal error - authentication returned unexpected format'
            }
        
        if not auth_result.get('valid', False):
            return {
                'success': False,
                'error': auth_result.get('error', 'Unknown authentication error'),
                'hint': 'Please check your API key and instance URL'
            }
    except Exception as e:
        return {
            'success': False,
            'error': f'Authentication failed: {str(e)}',
            'hint': 'Please check your network connection and credentials'
        }
    
    # Discover workspaces
    workspaces = await auth_client.discover_workspaces(instance, api_key)
    
    if not workspaces:
        return {
            'success': False,
            'error': 'No workspaces found or unable to list workspaces'
        }
    
    # If workspace_id provided, validate and use it
    if workspace_id:
        workspace = next((w for w in workspaces if w['id'] == workspace_id), None)
        if not workspace:
            return {
                'success': False,
                'error': f'Workspace {workspace_id} not found',
                'available_workspaces': [{'id': w['id'], 'name': w['name']} for w in workspaces]
            }
        
        # Set credentials
        auth_client.set_credentials(instance, workspace_id, api_key)
        
        # Save if requested
        if save_credentials:
            auth_client.storage.save_credentials(instance, workspace_id, api_key)
        
        return {
            'success': True,
            'authenticated': True,
            'instance': instance,
            'workspace': {
                'id': workspace['id'],
                'name': workspace['name']
            },
            'user': auth_result['user'],
            'credentials_saved': save_credentials
        }
    
    # No workspace specified - return list
    return {
        'success': True,
        'authenticated': False,
        'message': 'Authentication successful. Please select a workspace:',
        'available_workspaces': [
            {
                'id': w['id'],
                'name': w['name'],
                'description': w.get('description', '')
            }
            for w in workspaces
        ],
        'hint': 'Call authenticate_bigeye again with workspace_id parameter'
    }

@mcp.tool()
async def use_saved_credentials(
    instance: str,
    workspace_id: int
) -> Dict[str, Any]:
    """
    Use previously saved credentials for a Bigeye instance and workspace.
    """
    api_key = auth_client.storage.get_credentials(instance, workspace_id)
    
    if not api_key:
        return {
            'success': False,
            'error': f'No saved credentials for {instance} workspace {workspace_id}'
        }
    
    # Test that credentials still work
    auth_result = await auth_client.test_authentication(instance, api_key)
    
    if auth_result['valid']:
        auth_client.set_credentials(instance, workspace_id, api_key)
        return {
            'success': True,
            'message': f'Connected to {instance} workspace {workspace_id}',
            'user': auth_result['user']
        }
    else:
        return {
            'success': False,
            'error': 'Saved credentials are no longer valid',
            'hint': 'Please authenticate again with a new API key'
        }

@mcp.tool()
async def switch_workspace(workspace_id: int) -> Dict[str, Any]:
    """
    Switch to a different workspace in the current Bigeye instance.
    """
    if not auth_client.is_authenticated:
        return {
            'success': False,
            'error': 'Not authenticated. Please authenticate first.'
        }
    
    # Check if we have saved credentials for this workspace
    saved_key = auth_client.storage.get_credentials(
        auth_client.current_instance, 
        workspace_id
    )
    
    if saved_key:
        # Use saved credentials
        auth_client.set_credentials(
            auth_client.current_instance,
            workspace_id,
            saved_key
        )
        return {
            'success': True,
            'message': f'Switched to workspace {workspace_id} using saved credentials'
        }
    
    # Try with current API key
    workspaces = await auth_client.discover_workspaces(
        auth_client.current_instance,
        auth_client.api_key
    )
    
    workspace = next((w for w in workspaces if w['id'] == workspace_id), None)
    
    if workspace:
        auth_client.current_workspace_id = workspace_id
        # Save these credentials too
        auth_client.storage.save_credentials(
            auth_client.current_instance,
            workspace_id,
            auth_client.api_key
        )
        return {
            'success': True,
            'message': f'Switched to workspace: {workspace["name"]}'
        }
    else:
        return {
            'success': False,
            'error': f'Workspace {workspace_id} not accessible with current credentials'
        }

@mcp.tool()
async def list_workspaces() -> Dict[str, Any]:
    """
    List all available workspaces in the current Bigeye instance.
    """
    if not auth_client.api_key or not auth_client.current_instance:
        # Check for any saved credentials
        saved = auth_client.storage.list_saved_credentials()
        if saved:
            return {
                'authenticated': False,
                'saved_credentials': saved,
                'message': 'Not authenticated. Use saved credentials or authenticate with API key.'
            }
        else:
            return {
                'authenticated': False,
                'message': 'Not authenticated. Please provide API key.'
            }
    
    workspaces = await auth_client.discover_workspaces(
        auth_client.current_instance,
        auth_client.api_key
    )
    
    return {
        'authenticated': True,
        'current_workspace_id': auth_client.current_workspace_id,
        'workspaces': [
            {
                'id': w['id'],
                'name': w['name'],
                'description': w.get('description', ''),
                'is_current': w['id'] == auth_client.current_workspace_id
            }
            for w in workspaces
        ]
    }

@mcp.tool()
async def forget_credentials(
    instance: Optional[str] = None,
    workspace_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Remove saved credentials. If no parameters, removes all saved credentials.
    """
    auth_client.storage.delete_credentials(instance, workspace_id)
    
    if instance is None and workspace_id is None:
        return {
            'success': True,
            'message': 'All saved credentials have been removed'
        }
    elif instance and workspace_id:
        return {
            'success': True,
            'message': f'Removed credentials for {instance} workspace {workspace_id}'
        }
    elif instance:
        return {
            'success': True,
            'message': f'Removed all credentials for {instance}'
        }
    else:
        return {
            'success': False,
            'message': 'Invalid parameters. Specify both instance and workspace_id, just instance, or neither.'
        }

# Resources
@mcp.resource("bigeye://health")
async def get_health_resource() -> str:
    """Get the health status of the Bigeye API."""
    if not auth_client.is_authenticated:
        return "Not authenticated. Use authenticate_bigeye tool first."
    
    client = get_api_client()
    try:
        result = await client.check_health()
        return f"API Health Status: {result.get('status', 'Unknown')}"
    except Exception as e:
        return f"Error checking API health: {str(e)}"

@mcp.resource("bigeye://config")
def get_config_resource() -> Dict[str, Any]:
    """Get the current configuration for the Bigeye API connector."""
    if not auth_client.is_authenticated:
        return {
            "authenticated": False,
            "message": "Not authenticated. Use authenticate_bigeye tool first."
        }
    
    return {
        "authenticated": True,
        "instance": auth_client.current_instance,
        "workspace_id": auth_client.current_workspace_id,
        "api_base_url": auth_client.api_base_url
    }

@mcp.resource("bigeye://issues")
async def get_issues_resource() -> Dict[str, Any]:
    """Get all issues from the configured workspace."""
    if not auth_client.is_authenticated:
        return {
            "error": True,
            "message": "Not authenticated. Use authenticate_bigeye tool first."
        }
    
    client = get_api_client()
    debug_print(f"Fetching all issues for workspace {auth_client.current_workspace_id}")
    result = await client.fetch_issues(workspace_id=auth_client.current_workspace_id)
    
    issue_count = len(result.get("issues", []))
    debug_print(f"Found {issue_count} issues")
    
    return result

# Tools
@mcp.tool()
async def check_health() -> Dict[str, Any]:
    """Check the health of the Bigeye API."""
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
    client = get_api_client()
    debug_print(f"Fetching issues for workspace {auth_client.current_workspace_id}")
    
    if statuses:
        debug_print(f"Filtering by statuses: {statuses}")
    if schema_names:
        debug_print(f"Filtering by schema names: {schema_names}")
        
    result = await client.fetch_issues(
        workspace_id=auth_client.current_workspace_id,
        currentStatus=statuses,
        schemaNames=schema_names,
        page_size=page_size,
        page_cursor=page_cursor
    )
    
    issue_count = len(result.get("issues", []))
    debug_print(f"Found {issue_count} issues")
    
    return result

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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
            workspace_id=auth_client.current_workspace_id,
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
    # Validation
    if not issue_ids and not parent_issue_ids:
        return {
            "error": True,
            "message": "Either issue_ids or parent_issue_ids must be provided"
        }
    
    client = get_api_client()
    debug_print(f"Unmerging issues in workspace {auth_client.current_workspace_id}")
    
    try:
        result = await client.unmerge_issues(
            workspace_id=auth_client.current_workspace_id,
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
async def get_lineage_graph(
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
async def get_lineage_node(
    node_id: int
) -> Dict[str, Any]:
    """Get details for a specific lineage node to verify it exists and check its properties.
    
    This tool retrieves basic information about a lineage node.
    
    Args:
        node_id: The ID of the lineage node to get details for
        
    Returns:
        Dictionary containing the lineage node details
    """
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
async def get_lineage_node_issues(
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
async def analyze_upstream_root_causes(
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
    debug_print(f"Analyzing upstream root causes for node {node_id}")
    
    try:
        # Get upstream lineage graph
        upstream_result = await get_lineage_graph(
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
async def analyze_downstream_impact(
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
    debug_print(f"Analyzing downstream impact for node {node_id}")
    
    try:
        # Get downstream lineage graph
        downstream_result = await get_lineage_graph(
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
async def trace_issue_lineage_path(
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
    if not auth_client.is_authenticated:
        return {
            'error': 'Not authenticated',
            'hint': 'Use authenticate_bigeye tool first'
        }
    
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
        full_graph = await get_lineage_graph(
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
def authentication_flow() -> str:
    """Guide for authenticating with Bigeye."""
    return """
    # Authenticating with Bigeye
    
    This MCP server supports dynamic authentication without configuration files.
    
    ## First Time Authentication
    
    1. Get your API key from Bigeye (Settings > API Keys)
    2. Authenticate with your instance:
    
    ```python
    # For app.bigeye.com (default)
    result = await authenticate_bigeye(api_key="your_api_key_here")
    
    # For demo.bigeye.com - you can use any of these formats:
    result = await authenticate_bigeye(
        api_key="your_api_key_here",
        instance="demo"  # Automatically converts to https://demo.bigeye.com
    )
    # OR
    result = await authenticate_bigeye(
        api_key="your_api_key_here",
        instance="demo.bigeye.com"  # Automatically adds https://
    )
    # OR
    result = await authenticate_bigeye(
        api_key="your_api_key_here",
        instance="https://demo.bigeye.com"  # Full URL also works
    )
    ```
    
    ## Common Instance Names
    
    The tool automatically handles these common instances:
    - "demo" or "demo.bigeye.com" → https://demo.bigeye.com
    - "app", "app.bigeye.com", "prod", or "production" → https://app.bigeye.com
    - "staging" or "staging.bigeye.com" → https://staging.bigeye.com
    - Any other domain → automatically adds https://
    
    3. Select a workspace from the returned list:
    
    ```python
    # Authenticate with specific workspace
    result = await authenticate_bigeye(
        api_key="your_api_key_here",
        workspace_id=123456
    )
    ```
    
    ## Using Saved Credentials
    
    Once authenticated, credentials are saved securely for future use:
    
    ```python
    # List available saved credentials
    workspaces = await list_workspaces()
    
    # Use saved credentials
    result = await use_saved_credentials(
        instance="https://app.bigeye.com",
        workspace_id=123456
    )
    ```
    
    ## Switching Workspaces
    
    ```python
    # Switch to a different workspace in the same instance
    result = await switch_workspace(workspace_id=789012)
    ```
    
    ## Managing Credentials
    
    ```python
    # Remove specific credentials
    await forget_credentials(
        instance="https://demo.bigeye.com",
        workspace_id=123456
    )
    
    # Remove all saved credentials
    await forget_credentials()
    ```
    """

@mcp.prompt()
def check_connection_info() -> str:
    """Check the connection to Bigeye API."""
    return """
    Let's check if the Bigeye API connection is working:

    ```python
    # Check authentication status
    auth_status = await read_resource("bigeye://auth/status")
    print(auth_status)
    
    # If authenticated, check the current configuration
    config = await read_resource("bigeye://config")
    print(f"Instance: {config.get('instance')}")
    print(f"Workspace ID: {config.get('workspace_id')}")
    
    # Check health status
    health_status = await check_health()
    print(f"Health status: {health_status}")
    ```

    The authentication is dynamic - no environment variables or config files required!
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

# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()