"""
Bigeye API Client

This module provides functions to interact with the Bigeye API.
"""

import httpx
import sys
from typing import Dict, Any, Optional, List

class BigeyeAPIClient:
    """Client for interacting with the Bigeye API."""
    
    def __init__(self, api_url: str = "https://staging.bigeye.com", api_key: Optional[str] = None, workspace_id: Optional[int] = None):
        """Initialize the Bigeye API client.
        
        Args:
            api_url: The URL of the Bigeye API
            api_key: The API key for authentication
            workspace_id: The workspace ID to use for API requests
        """
        self.api_url = api_url
        self.api_key = api_key
        self.workspace_id = workspace_id
    
    async def make_request(
        self, 
        path: str, 
        method: str = "GET", 
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        timeout: float = 120.0
    ) -> Dict[str, Any]:
        """Make a request to the Bigeye API.
        
        Args:
            path: The API endpoint path
            method: The HTTP method (GET, POST, etc.)
            params: Query parameters
            json_data: JSON data for POST requests
            timeout: Request timeout in seconds (default: 60 seconds)
            
        Returns:
            The API response as a dictionary
        """
        url = f"{self.api_url}{path}"
        
        headers = {}
        if self.api_key:
            # don't change this to Bearer, it's apikey
            headers["Authorization"] = f"apikey {self.api_key}"
        
        # Add workspace_id as a header if configured
        if self.workspace_id:
            headers["x-bigeye-workspace-id"] = str(self.workspace_id)
        
        # Verbose logging for ALL requests
        print(f"\n[BIGEYE API VERBOSE] === REQUEST DETAILS ===", file=sys.stderr)
        print(f"[BIGEYE API VERBOSE] Method: {method}", file=sys.stderr)
        print(f"[BIGEYE API VERBOSE] URL: {url}", file=sys.stderr)
        print(f"[BIGEYE API VERBOSE] Headers: {headers}", file=sys.stderr)
        print(f"[BIGEYE API VERBOSE] Query params: {params}", file=sys.stderr)
        print(f"[BIGEYE API VERBOSE] JSON body: {json_data}", file=sys.stderr)
        
        # If we have params, construct the full URL with query string
        if params:
            from urllib.parse import urlencode
            query_string = urlencode(params)
            full_url = f"{url}?{query_string}"
            print(f"[BIGEYE API VERBOSE] Full URL with params: {full_url}", file=sys.stderr)
        
        # Create httpx client with timeout
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                if method == "GET":
                    response = await client.get(url, headers=headers, params=params)
                elif method == "POST":
                    # For search endpoint, add extra debugging
                    if "/api/v1/search" in url:
                        print(f"[BIGEYE API VERBOSE] SEARCH ENDPOINT DETECTED", file=sys.stderr)
                        import json as json_module
                        if json_data:
                            json_str = json_module.dumps(json_data, indent=2)
                            print(f"[BIGEYE API VERBOSE] JSON being sent:\n{json_str}", file=sys.stderr)
                        if params:
                            print(f"[BIGEYE API VERBOSE] Query params being sent: {params}", file=sys.stderr)
                    
                    response = await client.post(url, headers=headers, params=params, json=json_data)
                elif method == "PUT":
                    response = await client.put(url, headers=headers, json=json_data or params)
                elif method == "DELETE":
                    response = await client.delete(url, headers=headers, params=params)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                print(f"[BIGEYE API VERBOSE] Response status: {response.status_code}", file=sys.stderr)
                print(f"[BIGEYE API VERBOSE] Response headers: {dict(response.headers)}", file=sys.stderr)
                
                # Always log the raw response text for debugging
                raw_response = response.text
                print(f"[BIGEYE API VERBOSE] Raw response body: {raw_response[:1000]}..." if len(raw_response) > 1000 else f"[BIGEYE API VERBOSE] Raw response body: {raw_response}", file=sys.stderr)
                
                try:
                    if response.status_code >= 400:
                        print(f"[BIGEYE API VERBOSE] ERROR RESPONSE DETECTED", file=sys.stderr)
                        error_response = {
                            "error": True,
                            "status_code": response.status_code,
                            "message": response.text
                        }
                        print(f"[BIGEYE API VERBOSE] Returning error: {error_response}", file=sys.stderr)
                        return error_response
                    
                    result = response.json()
                    # Print first few items of response for debugging
                    print(f"[BIGEYE API DEBUG] Response preview: {str(result)[:200]}...", file=sys.stderr)
                    return result
                except Exception as e:
                    # Return text if not JSON
                    print(f"[BIGEYE API DEBUG] Exception parsing response: {str(e)}", file=sys.stderr)
                    return {
                        "raw_response": response.text,
                        "status_code": response.status_code
                    }
            except httpx.TimeoutException:
                print(f"[BIGEYE API DEBUG] Request timed out after {timeout} seconds", file=sys.stderr)
                return {
                    "error": True,
                    "message": f"Request timed out after {timeout} seconds"
                }
            except Exception as e:
                print(f"[BIGEYE API DEBUG] Request exception: {str(e)}", file=sys.stderr)
                return {
                    "error": True,
                    "message": f"Request failed: {str(e)}"
                }
    
    async def check_health(self) -> Dict[str, Any]:
        """Check the health of the Bigeye API."""
        try:
            result = await self.make_request("/health")
            return {
                "status": "healthy" if result.get("raw_response") == "OK" else "unhealthy",
                "response": result
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
            
    async def fetch_issues(
        self,
        workspace_id: int,
        currentStatus: Optional[List[str]] = None,
        schemaNames: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        page_cursor: Optional[str] = None,
        include_full_history: bool = False
    ) -> Dict[str, Any]:
        """Fetch issues from the Bigeye API.
        
        Args:
            workspace_id: The ID of the workspace to fetch issues from
            currentStatus: Optional list of issue statuses to filter by
                (e.g., ["ISSUE_STATUS_NEW", "ISSUE_STATUS_ACKNOWLEDGED"])
            schemaNames: Optional list of schema names to filter issues by
            page_size: Optional number of issues to return per page
            page_cursor: Cursor for pagination
            include_full_history: If False, strips out historical metric runs to reduce data size
            
        Returns:
            Dictionary containing the issues
        """
        payload = {
            "workspaceId": workspace_id
        }
        
        print(f"[BIGEYE API DEBUG] Fetching issues for workspace ID: {workspace_id}", file=sys.stderr)
        
        # Only add page_size if explicitly set
        if page_size is not None:
            payload["pageSize"] = page_size
        
        if currentStatus:
            payload["currentStatus"] = currentStatus
            
        if schemaNames:
            payload["schemaNames"] = schemaNames
            
        if page_cursor:
            payload["pageCursor"] = page_cursor
            
        result = await self.make_request(
            "/api/v1/issues/fetch",
            method="POST",
            json_data=payload
        )
        
        # If we want to reduce the response size, strip out the events/metric runs
        if not include_full_history and "issues" in result:
            filtered_issues = []
            for issue in result.get("issues", []):
                # Create a new dict with ONLY essential fields
                filtered_issue = {}
                essential_fields = [
                    "id", "name", "currentStatus", "priority", "description",
                    "tableName", "columnName", "schemaName", "warehouseName",
                    "createdAt", "updatedAt", "lastEventTime", "assignee",
                    "owner", "labels", "tags", "isIncident", "parentIssueId",
                    "alertId", "metricId", "tableId", "columnId"
                ]
                
                # Copy only essential fields that exist
                for field in essential_fields:
                    if field in issue:
                        filtered_issue[field] = issue[field]
                
                # Add simplified metric info if present
                if "metric" in issue and issue["metric"]:
                    filtered_issue["metric"] = {
                        "id": issue["metric"].get("id"),
                        "name": issue["metric"].get("name"),
                        "type": issue["metric"].get("type"),
                        "metricType": issue["metric"].get("metricType")
                    }
                
                # Add only the most recent event summary if events exist
                if "events" in issue and issue["events"] and len(issue["events"]) > 0:
                    most_recent_event = issue["events"][0]
                    filtered_issue["lastEvent"] = {
                        "type": most_recent_event.get("type"),
                        "timestamp": most_recent_event.get("timestamp"),
                        "message": most_recent_event.get("message")
                    }
                
                filtered_issues.append(filtered_issue)
            
            # Replace the original issues with filtered ones
            result["issues"] = filtered_issues
        
        return result
        
    async def merge_issues(
        self,
        issue_ids: List[int],
        workspace_id: int,
        existing_incident_id: Optional[int] = None,
        incident_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Merge multiple issues into a single incident.
        
        Args:
            issue_ids: List of issue IDs to merge
            workspace_id: The ID of the workspace containing the issues
            existing_incident_id: Optional ID of an existing incident to merge issues into
            incident_name: Optional name for the incident (new or existing)
            
        Returns:
            Dictionary containing the merge response with the created/updated incident
        """
        # Build the where clause with issue IDs and workspace ID
        where_clause = {
            "ids": issue_ids,
            "workspaceId": workspace_id
        }
        
        # Build the full request payload
        payload = {
            "where": where_clause
        }
        
        # Add existing incident ID if provided
        if existing_incident_id is not None:
            payload["existingIncident"] = existing_incident_id
        
        # Add incident name if provided
        if incident_name is not None:
            payload["incidentName"] = incident_name
        
        return await self.make_request(
            "/api/v1/issues/merge",
            method="POST",
            json_data=payload
        )

    async def get_upstream_issues_for_report(
        self,
        report_id: int
    ) -> Dict[str, Any]:
        """Get upstream issues for a BI report (like a Tableau workbook)

        Args:
            report_id: The ID of the BI report to get upstream issues for

        Returns:
            Dictionary containing the upstream issues of the desired report
        """
        return await self.make_request(
            f"/api/v2/lineage/nodes/{report_id}/upstream-issues",
            method="GET"
        )

    async def get_profile_for_table(
        self,
        table_id: int
    ) -> Dict[str, Any]:
        """Get profile report for a table

        Args:
            report_id: The ID of the BI report to get upstream issues for

        Returns:
            Dictionary containing the profile report of the desired table
        """
        return await self.make_request(
            f"/api/v2/tables/{table_id}/profile",
            method="GET"
        )

    async def get_profile_workflow_status_for_table(
        self,
        table_id: int
    ) -> Dict[str, Any]:
        """Get profile report workflow status for a given table

        Args:
            report_id: The ID of the table to queue profiling workflow status for

        Returns:
            Dictionary containing the workflow ID and status of the queued job
        """
        return await self.make_request(
            f"/api/v2/tables/{table_id}/profile/status",
            method="GET"
        )

    async def queue_table_profile(
        self,
        table_id: int
    ) -> Dict[str, Any]:
        """Queue a profiling job for a given table

        Args:
            table_id: The ID of the table to queue profiling for

        Returns:
            Dictionary containing the workflow ID of the queued job
        """

        payload = {}
        sampleSelection = {}
        sampleSelection["sampleMethod"] = "SAMPLE_METHOD_STRONGLY_RANDOM_SAMPLE"
        payload["sampleSelection"] = sampleSelection
        payload["tableId"] = table_id

        return await self.make_request(
            f"/api/v2/tables/{table_id}/profile/queue",
            method="POST",
            json_data=payload
        )
        
    async def get_issue_resolution_steps(
        self,
        issue_id: int
    ) -> Dict[str, Any]:
        """Get resolution steps for an issue or incident.
        
        Args:
            issue_id: The ID of the issue or incident to get resolution steps for
            
        Returns:
            Dictionary containing the resolution steps for the issue
        """
        return await self.make_request(
            f"/api/v1/issues/resolution/{issue_id}",
            method="POST"
        )
        
    async def update_issue(
        self,
        issue_id: int,
        new_status: Optional[str] = None,
        closing_label: Optional[str] = None,
        priority: Optional[str] = None,
        message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update an issue with status, priority, and/or add a timeline message.
        
        Args:
            issue_id: The ID of the issue to update
            new_status: New status - ISSUE_STATUS_NEW, ISSUE_STATUS_ACKNOWLEDGED, 
                       ISSUE_STATUS_CLOSED, ISSUE_STATUS_MONITORING, ISSUE_STATUS_MERGED
            closing_label: Required when new_status is ISSUE_STATUS_CLOSED - 
                          METRIC_RUN_LABEL_TRUE_NEGATIVE, METRIC_RUN_LABEL_FALSE_NEGATIVE,
                          METRIC_RUN_LABEL_TRUE_POSITIVE, METRIC_RUN_LABEL_FALSE_POSITIVE
            priority: New priority - ISSUE_PRIORITY_LOW, ISSUE_PRIORITY_MED, ISSUE_PRIORITY_HIGH
            message: Timeline message to add to the issue
            
        Returns:
            Dictionary containing the API response
        """
        payload = {}
        
        # Add status update if provided
        if new_status is not None:
            status_update = {"newStatus": new_status}

            # Add closing label if status is ISSUE_STATUS_CLOSED
            if new_status == "ISSUE_STATUS_CLOSED":
                if not closing_label:
                    raise ValueError("closing_label is required when new_status is ISSUE_STATUS_CLOSED")
                status_update["closingLabel"] = closing_label
                
            payload["statusUpdate"] = status_update


        # Add priority update if provided
        if priority is not None:
            payload["priorityUpdate"] = {"issuePriority": priority}
        
        # Add message update if provided
        if message is not None:
            payload["messageUpdate"] = {"message": message}
        
        # Ensure at least one update type is provided
        if not payload:
            raise ValueError("At least one update (new_status, priority, or message) must be provided")
        
        return await self.make_request(
            f"/api/v1/issues/{issue_id}",
            method="PUT",
            json_data=payload
        )
        
    async def unmerge_issues(
        self,
        workspace_id: int,
        issue_ids: Optional[List[int]] = None,
        parent_issue_ids: Optional[List[int]] = None,
        assignee_id: Optional[int] = None,
        new_status: Optional[str] = None
    ) -> Dict[str, Any]:
        """Unmerge issues from incidents.
        
        This function removes issues from incidents they've been merged into.
        You can specify issues to unmerge either by their IDs or by the incident ID they're part of.
        
        Args:
            issue_ids: Optional list of specific issue IDs to unmerge
            parent_issue_ids: Optional list of incident IDs to unmerge all issues from
            workspace_id: The ID of the workspace containing the issues
            assignee_id: Optional ID of user to assign the unmerged issues to
            new_status: Optional new status for unmerged issues 
                       (e.g., "ISSUE_STATUS_NEW", "ISSUE_STATUS_ACKNOWLEDGED")
            
        Returns:
            Dictionary containing the unmerge response
        """
        # Build the where clause
        where_clause = {
            "workspaceId": workspace_id
        }
        
        # Add issue IDs if provided
        if issue_ids:
            where_clause["ids"] = issue_ids
        
        # Add parent issue IDs if provided  
        if parent_issue_ids:
            where_clause["parentIssueIds"] = parent_issue_ids
        
        # Ensure at least one selection method is provided
        if not issue_ids and not parent_issue_ids:
            raise ValueError("Either issue_ids or parent_issue_ids must be provided")
        
        # Build the full request payload
        payload = {
            "where": where_clause
        }
        
        # Add assignee if provided
        if assignee_id is not None:
            payload["assignee"] = assignee_id
        
        # Add status if provided
        if new_status is not None:
            payload["status"] = new_status
        
        return await self.make_request(
            "/api/v1/issues/unmerge",
            method="POST",
            json_data=payload
        )
        
    async def get_lineage_graph(
        self,
        node_id: int,
        direction: str = "bidirectional",
        max_depth: Optional[int] = None,
        include_issues: bool = True
    ) -> Dict[str, Any]:
        """Get lineage graph for a data node.
        
        Args:
            node_id: The ID of the lineage node to get graph for
            direction: Direction to traverse - "upstream", "downstream", or "bidirectional"
            max_depth: Maximum depth to traverse (if not specified, uses API default)
            include_issues: Whether to include issue counts in the response
            
        Returns:
            Dictionary containing the lineage graph with nodes and relationships
        """
        params = {}
        
        # Map direction values to what the Java API expects
        if direction == "upstream":
            params["direction"] = "UPSTREAM"
        elif direction == "downstream":
            params["direction"] = "DOWNSTREAM"
        elif direction == "bidirectional":
            params["direction"] = "ALL"
        else:
            raise ValueError("direction must be 'upstream', 'downstream', or 'bidirectional'")
        
        # Use 'depth' parameter name as expected by Java API
        if max_depth is not None:
            params["depth"] = max_depth
            
        # Note: The Java API doesn't appear to have an includeIssues parameter
        # Issue counts are included by default in the response
        
        return await self.make_request(
            f"/api/v2/lineage/nodes/{node_id}/graph",
            method="GET",
            params=params
        )
        
    async def get_lineage_node(
        self,
        node_id: int
    ) -> Dict[str, Any]:
        """Get details for a specific lineage node.
        
        Args:
            node_id: The ID of the lineage node to get details for
            
        Returns:
            Dictionary containing the lineage node details
        """
        return await self.make_request(
            f"/api/v2/lineage/nodes/{node_id}",
            method="GET"
        )
        
    async def get_lineage_node_issues(
        self,
        node_id: int
    ) -> Dict[str, Any]:
        """Get all issues affecting a specific lineage node.
        
        Args:
            node_id: The ID of the lineage node to get issues for
            
        Returns:
            Dictionary containing issues for the lineage node
        """
        return await self.make_request(
            f"/api/v2/lineage/nodes/{node_id}/issues",
            method="GET"
        )
        
    async def get_upstream_applicable_metrics(
        self,
        node_id: int
    ) -> Dict[str, Any]:
        """Get applicable metric types for upstream analysis of a lineage node.
        
        Args:
            node_id: The ID of the lineage node
            
        Returns:
            Dictionary containing applicable upstream metric types
        """
        return await self.make_request(
            f"/api/v2/lineage/nodes/{node_id}/upstream-applicable-metric-types",
            method="GET"
        )
        
    async def create_lineage_node(
        self,
        node_name: str,
        node_container_name: str,
        node_type: str = "DATA_NODE_TYPE_CUSTOM",
        workspace_id: Optional[int] = None,
        rebuild_graph: bool = True
    ) -> Dict[str, Any]:
        """Create a custom lineage node.
        
        Args:
            node_name: Name of the node (e.g., "AI Agent - Claude")
            node_container_name: Container name for the node (e.g., "MCP Server", "Python")
            node_type: Type of node (default: "DATA_NODE_TYPE_CUSTOM")
            workspace_id: Optional workspace ID
            rebuild_graph: Whether to rebuild the lineage graph after creating the node
            
        Returns:
            Dictionary containing the created node details
        """
        payload = {
            "nodeType": node_type,
            "nodeName": node_name,
            "nodeContainerName": node_container_name,
            "rebuildGraph": rebuild_graph
        }
        
        if workspace_id is not None:
            payload["workspaceId"] = workspace_id
            
        return await self.make_request(
            "/api/v2/lineage/nodes",
            method="POST",
            json_data=payload
        )
        
    async def create_lineage_edge(
        self,
        upstream_node_id: int,
        downstream_node_id: int,
        relationship_type: str = "RELATIONSHIP_TYPE_LINEAGE",
        rebuild_graph: bool = True
    ) -> Dict[str, Any]:
        """Create a lineage edge between two nodes.
        
        Args:
            upstream_node_id: ID of the upstream node (data source)
            downstream_node_id: ID of the downstream node (data consumer)
            relationship_type: Type of relationship (default: "RELATIONSHIP_TYPE_LINEAGE")
            rebuild_graph: Whether to rebuild the lineage graph after creating the edge
            
        Returns:
            Dictionary containing the created edge details
        """
        payload = {
            "upstreamDataNodeId": upstream_node_id,
            "downstreamDataNodeId": downstream_node_id,
            "relationshipType": relationship_type,
            "rebuildGraph": rebuild_graph
        }
        
        return await self.make_request(
            "/api/v2/lineage/edges",
            method="POST",
            json_data=payload
        )
        
    async def find_lineage_node_by_name(
        self,
        node_name: str,
        node_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Find a lineage node by name.
           Note: do not include asterisks to search.
           The server includes wildcards automatically.
        
        Args:
            node_name: Name of the node to find
            node_type: Optional node type to filter by
            
        Returns:
            Dictionary containing matching nodes
        """
        params = {
            "nodeName": node_name
        }
        
        if node_type:
            params["nodeType"] = node_type
            
        result = await self.make_request(
            "/api/v2/lineage/nodes/search",
            method="GET",
            params=params
        )
        
        # If we get a 404, try without node type as fallback
        if result.get("error") and result.get("status_code") == 404 and node_type:
            print(f"[BIGEYE API DEBUG] Retrying search without node type filter", file=sys.stderr)
            params = {"nodeName": node_name}
            result = await self.make_request(
                "/api/v2/lineage/nodes/search",
                method="GET",
                params=params
            )
            
        return result
        
    async def get_lineage_node_by_entity_id(
        self,
        entity_id: int
    ) -> Dict[str, Any]:
        """Get a lineage node by its entity ID.
        
        Args:
            entity_id: The entity ID of the node
            
        Returns:
            Dictionary containing the node details
        """
        # Try to get nodes for this entity
        result = await self.make_request(
            f"/api/v2/lineage/nodes/entity/{entity_id}",
            method="GET"
        )
        
        # If that endpoint doesn't exist, try search
        if result.get("error") and result.get("status_code") == 404:
            # Search for nodes without name filter
            all_nodes = await self.make_request(
                "/api/v2/lineage/nodes",
                method="GET"
            )
            
            if not all_nodes.get("error"):
                nodes = all_nodes.get("nodes", [])
                for node in nodes:
                    if node.get("nodeEntityId") == entity_id:
                        return {"nodes": [node]}
                        
        return result
        
    async def find_table_lineage_node(
        self,
        database: str,
        schema: str,
        table: str
    ) -> Dict[str, Any]:
        """Find a lineage node for a specific table.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            
        Returns:
            Dictionary containing the table's lineage node if found
        """
        # Try different name formats that Bigeye might use
        name_formats = [
            f"{database}.{schema}.{table}",  # Standard 3-part name
            f"{schema}.{table}",              # 2-part name (schema.table)
            f"{table}",                       # Just table name
            f"SNOWFLAKE.{database}.{schema}.{table}",  # With warehouse prefix
        ]
        
        print(f"[BIGEYE API DEBUG] Trying to find table with formats: {name_formats}", file=sys.stderr)
        
        # Try each format
        for name_format in name_formats:
            full_table_name = name_format.upper()
            print(f"[BIGEYE API DEBUG] Searching for table: {full_table_name}", file=sys.stderr)
            
            result = await self.make_request(
                "/api/v2/lineage/nodes/search",
                method="GET",
                params={
                    "nodeName": full_table_name,
                    "nodeType": "DATA_NODE_TYPE_TABLE"
                }
            )
            
            # Check if we found the table
            if result and not result.get("error"):
                nodes = result.get("nodes", [])
                if nodes:
                    print(f"[BIGEYE API DEBUG] Found table with format: {full_table_name}", file=sys.stderr)
                    return result
        
        # If none of the formats worked, return the last error
        print(f"[BIGEYE API DEBUG] Table not found with any format", file=sys.stderr)
        return result
        
    async def search_lineage_nodes_by_pattern(
        self,
        pattern: str,
        node_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Search for lineage nodes by pattern without strict matching.
        
        Args:
            pattern: Search pattern for node names
            node_type: Optional node type filter
            
        Returns:
            Dictionary containing matching nodes
        """
        params = {
            "nodeName": pattern.upper()
        }
        
        if node_type:
            params["nodeType"] = node_type
            
        print(f"[BIGEYE API DEBUG] Searching nodes with pattern: {pattern}, type: {node_type}", file=sys.stderr)
        
        return await self.make_request(
            "/api/v2/lineage/nodes/search",
            method="GET",
            params=params
        )
        
    async def find_column_lineage_node(
        self,
        database: str,
        schema: str,
        table: str,
        column: str
    ) -> Dict[str, Any]:
        """Find a lineage node for a specific column.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            column: Column name
            
        Returns:
            Dictionary containing the column's lineage node if found
        """
        # Search for the column using its fully qualified name
        full_column_name = f"{database}.{schema}.{table}.{column}".upper()
        
        return await self.make_request(
            "/api/v2/lineage/nodes/search",
            method="GET",
            params={
                "nodeName": full_column_name,
                "nodeType": "DATA_NODE_TYPE_COLUMN"
            }
        )
        
    async def get_lineage_edges_for_node(
        self,
        node_id: int,
        direction: str = "both"
    ) -> Dict[str, Any]:
        """Get all lineage edges connected to a node.
        
        Args:
            node_id: The lineage node ID
            direction: Direction to search ("upstream", "downstream", or "both")
            
        Returns:
            Dictionary containing edges connected to the node
        """
        # Note: This endpoint might not exist in the current Bigeye API
        # If it doesn't exist, we might need to use get_lineage_graph and extract edges
        try:
            # First try a direct edge endpoint if it exists
            return await self.make_request(
                f"/api/v2/lineage/nodes/{node_id}/edges",
                method="GET",
                params={"direction": direction}
            )
        except Exception:
            # Fallback: Use lineage graph and extract edges
            graph = await self.get_lineage_graph(
                node_id=node_id,
                direction=direction,
                max_depth=1
            )
            
            # Extract edges from the graph
            edges = []
            if graph and "nodes" in graph:
                for node_data in graph["nodes"].values():
                    if "upstreamEdges" in node_data:
                        edges.extend(node_data["upstreamEdges"])
                    if "downstreamEdges" in node_data:
                        edges.extend(node_data["downstreamEdges"])
                        
            return {"edges": edges}
            
    async def delete_lineage_edge(
        self,
        edge_id: int
    ) -> Dict[str, Any]:
        """Delete a lineage edge.
        
        Args:
            edge_id: The ID of the edge to delete
            
        Returns:
            Dictionary containing deletion status
        """
        return await self.make_request(
            f"/api/v2/lineage/edges/{edge_id}",
            method="DELETE"
        )
        
    async def get_catalog_tables(
        self,
        workspace_id: int,
        schema_name: Optional[str] = None,
        warehouse_name: Optional[str] = None,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """Get tables from Bigeye's catalog.
        
        Args:
            workspace_id: The workspace ID
            schema_name: Optional schema name to filter by
            warehouse_name: Optional warehouse name to filter by
            page_size: Number of results per page
            
        Returns:
            Dictionary containing catalog tables
        """
        payload = {
            "workspaceId": workspace_id,
            "pageSize": page_size
        }
        
        if schema_name:
            payload["schemaName"] = schema_name
            
        if warehouse_name:
            payload["warehouseName"] = warehouse_name
            
        return await self.make_request(
            "/api/v1/catalog/tables",
            method="POST",
            json_data=payload
        )
        
    async def get_issues_for_table(
        self,
        workspace_id: int,
        table_name: str,
        warehouse_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        currentStatus: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get issues for a specific table.
        
        Args:
            workspace_id: The workspace ID
            table_name: Table name to filter by
            warehouse_name: Optional warehouse name
            schema_name: Optional schema name
            currentStatus: Optional list of issue statuses
            
        Returns:
            Dictionary containing issues for the table
        """
        # First, try to find the table in the catalog
        catalog_result = await self.get_catalog_tables(
            workspace_id=workspace_id,
            schema_name=schema_name,
            warehouse_name=warehouse_name,
            page_size=100
        )
        
        if catalog_result.get("error"):
            return catalog_result
            
        tables = catalog_result.get("tables", [])
        
        # Find the matching table
        matching_table = None
        for table in tables:
            if table.get("tableName", "").upper() == table_name.upper():
                matching_table = table
                break
                
        if not matching_table:
            return {
                "error": True,
                "message": f"Table {table_name} not found in catalog"
            }
            
        # Get the table's ID and schema
        table_id = matching_table.get("id")
        table_schema = matching_table.get("schemaName")
        
        print(f"[BIGEYE API DEBUG] Found table {table_name} with ID {table_id} in schema {table_schema}", file=sys.stderr)
        
        # Now fetch issues for this specific schema/table
        payload = {
            "workspaceId": workspace_id
        }
        
        if table_schema:
            payload["schemaNames"] = [table_schema]
            
        if currentStatus:
            payload["currentStatus"] = currentStatus
            
        # Fetch all issues for the schema
        issues_result = await self.make_request(
            "/api/v1/issues/fetch",
            method="POST",
            json_data=payload
        )
        
        if issues_result.get("error"):
            return issues_result
            
        all_issues = issues_result.get("issues", [])
        
        # Strip out excessive historical data from issues - create new filtered issues
        filtered_issues = []
        essential_fields = [
            "id", "name", "currentStatus", "priority", "description",
            "tableName", "columnName", "schemaName", "warehouseName",
            "createdAt", "updatedAt", "lastEventTime", "assignee",
            "owner", "labels", "tags", "isIncident", "parentIssueId",
            "alertId", "metricId", "tableId", "columnId"
        ]
        
        for issue in all_issues:
            # Create a new dict with ONLY essential fields
            filtered_issue = {}
            
            # Copy only essential fields that exist
            for field in essential_fields:
                if field in issue:
                    filtered_issue[field] = issue[field]
            
            # Add simplified metric info if present
            if "metric" in issue and issue["metric"]:
                filtered_issue["metric"] = {
                    "id": issue["metric"].get("id"),
                    "name": issue["metric"].get("name"),
                    "type": issue["metric"].get("type"),
                    "metricType": issue["metric"].get("metricType")
                }
            
            # Add only the most recent event summary if events exist
            if "events" in issue and issue["events"] and len(issue["events"]) > 0:
                most_recent_event = issue["events"][0]
                filtered_issue["lastEvent"] = {
                    "type": most_recent_event.get("type"),
                    "timestamp": most_recent_event.get("timestamp"),
                    "message": most_recent_event.get("message")
                }
            
            filtered_issues.append(filtered_issue)
        
        # Use filtered issues from here on
        all_issues = filtered_issues
        
        # Filter to only issues for this specific table
        table_issues = []
        for issue in all_issues:
            # Check if issue is related to this table
            metric = issue.get("metric", {})
            if metric:
                metric_table = metric.get("tableName", "")
                if metric_table.upper() == table_name.upper():
                    table_issues.append(issue)
                    
        return {
            "table": table_name,
            "schema": table_schema,
            "total_issues": len(table_issues),
            "issues": table_issues
        }
        
    async def get_table_metrics(
        self,
        workspace_id: int,
        table_name: str,
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get metrics configured for a specific table.
        
        Args:
            workspace_id: The workspace ID
            table_name: Table name
            schema_name: Optional schema name
            
        Returns:
            Dictionary containing metrics for the table
        """
        # Build the API path - this might need adjustment based on actual API
        params = {
            "workspaceId": workspace_id,
            "tableName": table_name
        }
        
        if schema_name:
            params["schemaName"] = schema_name
            
        return await self.make_request(
            "/api/v1/metrics",
            method="GET",
            params=params
        )
        
    async def delete_lineage_node(
        self,
        node_id: int,
        force: bool = False
    ) -> Dict[str, Any]:
        """Delete a custom lineage node.
        
        Args:
            node_id: The ID of the lineage node to delete
            force: Force deletion even if node has active edges (the API may not support this)
            
        Returns:
            Dictionary containing deletion status
        """
        # Note: The force parameter might not be supported by the API
        # but we include it for future compatibility
        params = {}
        if force:
            params["force"] = "true"
            
        return await self.make_request(
            f"/api/v2/lineage/nodes/{node_id}",
            method="DELETE",
            params=params if params else None
        )
    
    async def search_schemas(
        self,
        workspace_id: int,
        schema_name: Optional[str] = None,
        warehouse_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """Search for schemas in Bigeye.
        
        Args:
            workspace_id: Required workspace ID for the search
            schema_name: Optional schema name to filter by (supports partial matching)
            warehouse_ids: Optional list of warehouse IDs to filter by
            
        Returns:
            Dictionary containing schemas
        """
        params = {
            "workspaceId": workspace_id
        }
        
        if schema_name:
            params["schema"] = schema_name
            
        if warehouse_ids:
            params["warehouseId"] = warehouse_ids
            
        print(f"[BIGEYE API DEBUG] Schema search params: {params}", file=sys.stderr)
        
        return await self.make_request(
            "/api/v1/schemas",
            method="GET",
            params=params
        )
    
    async def search_tables(
        self,
        workspace_id: int,
        table_name: Optional[str] = None,
        schema_names: Optional[List[str]] = None,
        warehouse_ids: Optional[List[int]] = None,
        include_columns: bool = False
    ) -> Dict[str, Any]:
        """Search for tables in Bigeye.
        
        Args:
            workspace_id: Required workspace ID for the search
            table_name: Optional table name to filter by (supports partial matching)
            schema_names: Optional list of schema names to filter by
            warehouse_ids: Optional list of warehouse IDs to filter by
            include_columns: Whether to include column information in the response
            
        Returns:
            Dictionary containing tables
        """
        params = {
            "workspaceId": workspace_id
        }
        
        if table_name:
            params["tableName"] = table_name
            
        if schema_names:
            params["schema"] = schema_names
            
        if warehouse_ids:
            params["warehouseId"] = warehouse_ids
            
        if not include_columns:
            params["ignoreFields"] = True
            
        print(f"[BIGEYE API DEBUG] Table search params: {params}", file=sys.stderr)
        
        return await self.make_request(
            "/api/v1/tables",
            method="GET",
            params=params
        )
    
    async def search_columns(
        self,
        workspace_id: int,
        column_name: Optional[str] = None,
        table_names: Optional[List[str]] = None,
        schema_names: Optional[List[str]] = None,
        warehouse_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """Search for columns in Bigeye.
        
        Args:
            workspace_id: Required workspace ID for the search
            column_name: Optional column name to filter by (supports partial matching)
            table_names: Optional list of table names to filter by
            schema_names: Optional list of schema names to filter by
            warehouse_ids: Optional list of warehouse IDs to filter by
            
        Returns:
            Dictionary containing columns
        """
        params = {
            "workspaceId": workspace_id
        }
        
        if column_name:
            params["columnName"] = column_name
            
        if table_names:
            params["tableName"] = table_names
            
        if schema_names:
            params["schema"] = schema_names
            
        if warehouse_ids:
            params["warehouseId"] = warehouse_ids
            
        print(f"[BIGEYE API DEBUG] Column search params: {params}", file=sys.stderr)
        
        return await self.make_request(
            "/api/v1/columns",
            method="GET",
            params=params
        )
        
    async def search_lineage_v2(
        self,
        search_string: str,
        workspace_id: int,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Search for lineage nodes using the v2 search API.
        
        This uses Bigeye's path-based search format where you can search using:
        - warehouse/schema/table/column format
        - Partial names with wildcards (*)
        - Individual component names
        
        Args:
            search_string: Search string in path format (e.g., "SNOWFLAKE/SALES/ORDERS")
            workspace_id: The workspace ID to search in
            limit: Maximum number of results (default: 100)
            
        Returns:
            Dictionary containing search results with node details
            
        Examples:
            - "SNOWFLAKE/SALES" - Find all objects in SNOWFLAKE.SALES schema
            - "*/ORDERS" - Find all ORDERS tables across any schema
            - "CUSTOMER*" - Find all objects starting with CUSTOMER
            - "PROD_REPL/DIM_CUSTOMER/CUSTOMER_ID" - Find specific column
        """
        print(f"[BIGEYE API DEBUG] === search_lineage_v2 called ===", file=sys.stderr)
        print(f"[BIGEYE API DEBUG] Parameters:", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   search_string: '{search_string}' (type: {type(search_string)})", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   workspace_id: {workspace_id} (type: {type(workspace_id)})", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   limit: {limit} (type: {type(limit)})", file=sys.stderr)
        
        # Ensure workspace_id is an integer
        try:
            workspace_id = int(workspace_id)
            print(f"[BIGEYE API DEBUG] Converted workspace_id to int: {workspace_id}", file=sys.stderr)
        except Exception as e:
            print(f"[BIGEYE API DEBUG] ERROR converting workspace_id: {e}", file=sys.stderr)
            return {
                "error": True,
                "message": f"Invalid workspace_id: {workspace_id} - must be an integer"
            }
        
        payload = {
            "search": search_string,
            "workspaceId": workspace_id,
            "limit": limit
        }
        
        print(f"[BIGEYE API DEBUG] Created payload:", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   {payload}", file=sys.stderr)
        print(f"[BIGEYE API DEBUG] Payload keys: {list(payload.keys())}", file=sys.stderr)
        print(f"[BIGEYE API DEBUG] Calling make_request with:", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   path: /api/v2/lineage/search", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   method: POST", file=sys.stderr)
        print(f"[BIGEYE API DEBUG]   json_data: {payload}", file=sys.stderr)
        
        return await self.make_request(
            "/api/v2/lineage/search",
            method="POST",
            json_data=payload
        )