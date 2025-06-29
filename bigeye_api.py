"""
Bigeye API Client

This module provides functions to interact with the Bigeye API.
"""

import httpx
import sys
from typing import Dict, Any, Optional, List

class BigeyeAPIClient:
    """Client for interacting with the Bigeye API."""
    
    def __init__(self, api_url: str = "https://staging.bigeye.com", api_key: Optional[str] = None):
        """Initialize the Bigeye API client.
        
        Args:
            api_url: The URL of the Bigeye API
            api_key: The API key for authentication
        """
        self.api_url = api_url
        self.api_key = api_key
    
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
        
        # Create httpx client with timeout
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                if method == "GET":
                    response = await client.get(url, headers=headers, params=params)
                elif method == "POST":
                    response = await client.post(url, headers=headers, json=json_data or params)
                elif method == "PUT":
                    response = await client.put(url, headers=headers, json=json_data or params)
                elif method == "DELETE":
                    response = await client.delete(url, headers=headers, params=params)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                
                print(f"[BIGEYE API DEBUG] Response status: {response.status_code}", file=sys.stderr)
                
                try:
                    if response.status_code >= 400:
                        error_response = {
                            "error": True,
                            "status_code": response.status_code,
                            "message": response.text
                        }
                        print(f"[BIGEYE API DEBUG] Error response: {error_response}", file=sys.stderr)
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
        page_cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch issues from the Bigeye API.
        
        Args:
            workspace_id: The ID of the workspace to fetch issues from
            currentStatus: Optional list of issue statuses to filter by
                (e.g., ["ISSUE_STATUS_NEW", "ISSUE_STATUS_ACKNOWLEDGED"])
            schemaNames: Optional list of schema names to filter issues by
            page_size: Optional number of issues to return per page
            page_cursor: Cursor for pagination
            
        Returns:
            Dictionary containing the issues
        """
        payload = {
            "workspaceId": workspace_id
        }
        
        # Only add page_size if explicitly set
        if page_size is not None:
            payload["pageSize"] = page_size
        
        if currentStatus:
            payload["currentStatus"] = currentStatus
            
        if schemaNames:
            payload["schemaNames"] = schemaNames
            
        if page_cursor:
            payload["pageCursor"] = page_cursor
            
        return await self.make_request(
            "/api/v1/issues/fetch",
            method="POST",
            json_data=payload
        )
        
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