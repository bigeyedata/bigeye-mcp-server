"""
Lineage Tracking Module for Bigeye MCP Server

This module handles tracking of data access by AI agents and creates
lineage relationships in Bigeye. It's designed to work across all
database MCP servers (Snowflake, Databricks, etc.).
"""

import os
import sys
import re
from typing import Dict, Set, Optional, List, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

class AgentLineageTracker:
    """Tracks data access by AI agents and manages lineage relationships with Bigeye."""
    
    def __init__(self, bigeye_client=None, agent_name: str = None, workspace_id: int = None, debug: bool = False):
        """Initialize the lineage tracker.
        
        Args:
            bigeye_client: Bigeye API client instance
            agent_name: Name of the AI agent (defaults to environment-based name)
            workspace_id: Bigeye workspace ID
            debug: Enable debug logging
        """
        self.bigeye_client = bigeye_client
        self.workspace_id = workspace_id
        self.debug = debug
        
        # Set agent name from environment or use default
        self.agent_name = agent_name or os.environ.get(
            "MCP_AGENT_NAME", 
            f"AI Agent - {os.environ.get('USER', 'Unknown')}"
        )
        
        # Track accessed data assets in this session
        # Format: {database: {schema: {table: Set[columns]}}}
        self.accessed_assets: Dict[str, Dict[str, Dict[str, Set[str]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(set))
        )
        
        # Cache for node IDs to avoid repeated lookups
        self.node_cache: Dict[str, int] = {}
        
        # Agent node ID (will be created/retrieved on first use)
        self.agent_node_id: Optional[int] = None
        
        # Track which edges we've already created to avoid duplicates
        self.created_edges: Set[Tuple[int, int]] = set()
        
    def debug_print(self, message: str):
        """Print debug messages to stderr."""
        if self.debug:
            print(f"[AGENT LINEAGE] {message}", file=sys.stderr)
            
    def parse_qualified_name(self, qualified_name: str) -> Optional[Dict[str, str]]:
        """Parse a fully qualified column or table name.
        
        Supports formats:
        - database.schema.table
        - database.schema.table.column
        - warehouse.database.schema.table
        - warehouse.database.schema.table.column
        
        Args:
            qualified_name: Fully qualified name
            
        Returns:
            Dictionary with parsed components or None if invalid
        """
        parts = qualified_name.split(".")
        
        if len(parts) == 3:
            # database.schema.table
            return {
                "warehouse": None,
                "database": parts[0],
                "schema": parts[1],
                "table": parts[2],
                "column": None
            }
        elif len(parts) == 4:
            # Could be warehouse.database.schema.table OR database.schema.table.column
            # We'll need context to determine which, for now assume column if last part is lowercase
            if parts[-1].islower() or "_" in parts[-1]:
                # Likely a column name
                return {
                    "warehouse": None,
                    "database": parts[0],
                    "schema": parts[1],
                    "table": parts[2],
                    "column": parts[3]
                }
            else:
                # Likely warehouse.database.schema.table
                return {
                    "warehouse": parts[0],
                    "database": parts[1],
                    "schema": parts[2],
                    "table": parts[3],
                    "column": None
                }
        elif len(parts) == 5:
            # warehouse.database.schema.table.column
            return {
                "warehouse": parts[0],
                "database": parts[1],
                "schema": parts[2],
                "table": parts[3],
                "column": parts[4]
            }
        else:
            self.debug_print(f"Invalid qualified name format: {qualified_name}")
            return None
            
    def track_asset_access(self, qualified_names: List[str]):
        """Track access to data assets (tables/columns).
        
        Args:
            qualified_names: List of fully qualified names of accessed assets
        """
        for name in qualified_names:
            parsed = self.parse_qualified_name(name)
            if parsed:
                database = parsed["database"].upper()
                schema = parsed["schema"].upper()
                table = parsed["table"].upper()
                column = parsed["column"].upper() if parsed["column"] else None
                
                if column:
                    self.accessed_assets[database][schema][table].add(column)
                    self.debug_print(f"Tracked column access: {database}.{schema}.{table}.{column}")
                else:
                    # Table access without specific columns means all columns
                    self.accessed_assets[database][schema][table].add("*")
                    self.debug_print(f"Tracked table access: {database}.{schema}.{table}")
                    
    def get_tracked_assets(self) -> Dict[str, Any]:
        """Get a summary of all tracked assets.
        
        Returns:
            Dictionary with tracked tables and columns
        """
        result = {
            "tables": [],
            "total_tables": 0,
            "total_columns": 0
        }
        
        for database, schemas in self.accessed_assets.items():
            for schema, tables in schemas.items():
                for table, columns in tables.items():
                    table_info = {
                        "database": database,
                        "schema": schema,
                        "table": table,
                        "columns": list(columns) if "*" not in columns else ["*"]
                    }
                    result["tables"].append(table_info)
                    result["total_tables"] += 1
                    if "*" not in columns:
                        result["total_columns"] += len(columns)
                        
        return result
        
    def clear_tracked_assets(self):
        """Clear all tracked assets and edge cache."""
        self.accessed_assets.clear()
        self.created_edges.clear()
        self.debug_print("Cleared all tracked assets")
        
    async def ensure_agent_node(self) -> Optional[int]:
        """Ensure the AI agent node exists in Bigeye lineage.
        
        Returns:
            The node ID of the agent, or None if creation failed
        """
        if not self.bigeye_client:
            self.debug_print("No Bigeye client configured")
            return None
            
        if self.agent_node_id:
            return self.agent_node_id
            
        try:
            # First, try to find existing node
            self.debug_print(f"Looking for existing agent node: {self.agent_name}")
            search_result = await self.bigeye_client.find_lineage_node_by_name(
                node_name=self.agent_name,
                node_type="DATA_NODE_TYPE_CUSTOM"
            )
            
            if search_result and not search_result.get("error"):
                nodes = search_result.get("nodes", [])
                if nodes:
                    self.agent_node_id = nodes[0]["id"]
                    self.debug_print(f"Found existing agent node with ID: {self.agent_node_id}")
                    return self.agent_node_id
            
            # Create new agent node
            self.debug_print(f"Creating new agent node: {self.agent_name}")
            create_result = await self.bigeye_client.create_lineage_node(
                node_name=self.agent_name,
                node_container_name="AI Agents",
                node_type="DATA_NODE_TYPE_CUSTOM",
                workspace_id=self.workspace_id,
                rebuild_graph=False
            )
            
            if create_result and not create_result.get("error"):
                self.agent_node_id = create_result.get("id")
                self.debug_print(f"Created agent node with ID: {self.agent_node_id}")
                return self.agent_node_id
            else:
                # Check if error is about existing node
                error_msg = str(create_result.get("message", ""))
                if "already exists" in error_msg:
                    # Extract entity ID if possible
                    import re
                    match = re.search(r'DataNodeEntity\((\d+)\)', error_msg)
                    if match:
                        entity_id = int(match.group(1))
                        self.debug_print(f"Node already exists with entity ID: {entity_id}")
                        
                        # Try to get node by entity ID
                        entity_result = await self.bigeye_client.get_lineage_node_by_entity_id(entity_id)
                        if entity_result and not entity_result.get("error"):
                            nodes = entity_result.get("nodes", [])
                            if nodes:
                                self.agent_node_id = nodes[0]["id"]
                                self.debug_print(f"Found existing agent node via entity ID with ID: {self.agent_node_id}")
                                return self.agent_node_id
                                
                    # As last resort, try searching without node type
                    self.debug_print("Trying to find node without type filter")
                    search_result = await self.bigeye_client.find_lineage_node_by_name(
                        node_name=self.agent_name,
                        node_type=None  # No type filter
                    )
                    
                    if search_result and not search_result.get("error"):
                        nodes = search_result.get("nodes", [])
                        if nodes:
                            self.agent_node_id = nodes[0]["id"]
                            self.debug_print(f"Found existing agent node without type filter with ID: {self.agent_node_id}")
                            return self.agent_node_id
                
                self.debug_print(f"Failed to create agent node: {create_result}")
                
        except Exception as e:
            self.debug_print(f"Error ensuring agent node: {str(e)}")
            
        return None
        
    async def find_asset_node_id(self, database: str, schema: str, table: str, column: Optional[str] = None) -> Optional[int]:
        """Find the Bigeye node ID for a data asset.
        
        Args:
            database: Database name
            schema: Schema name
            table: Table name
            column: Optional column name
            
        Returns:
            The node ID if found, None otherwise
        """
        if not self.bigeye_client:
            return None
            
        # Create cache key
        if column:
            cache_key = f"{database}.{schema}.{table}.{column}".upper()
            asset_type = "column"
        else:
            cache_key = f"{database}.{schema}.{table}".upper()
            asset_type = "table"
            
        # Check cache
        if cache_key in self.node_cache:
            return self.node_cache[cache_key]
            
        try:
            if column:
                # For columns, we need to find the table first, then the column
                self.debug_print(f"Searching for column in Bigeye: {cache_key}")
                # Note: This assumes Bigeye has a method to find column nodes
                # If not, we may need to use table-level lineage only
                result = await self.bigeye_client.find_column_lineage_node(
                    database=database,
                    schema=schema,
                    table=table,
                    column=column
                )
            else:
                # For tables
                self.debug_print(f"Searching for table in Bigeye: {cache_key}")
                result = await self.bigeye_client.find_table_lineage_node(
                    database=database,
                    schema=schema,
                    table=table
                )
                
            if result and not result.get("error"):
                nodes = result.get("nodes", [])
                if nodes:
                    node_id = nodes[0]["id"]
                    self.node_cache[cache_key] = node_id
                    self.debug_print(f"Found {asset_type} node {cache_key} with ID: {node_id}")
                    return node_id
                else:
                    self.debug_print(f"{asset_type.capitalize()} not found in Bigeye catalog: {cache_key}")
            else:
                error_msg = result.get("error", "Unknown error") if result else "No response"
                self.debug_print(f"Error from Bigeye API: {error_msg}")
                
        except Exception as e:
            self.debug_print(f"Error finding {asset_type} node: {str(e)}")
            
        return None
        
    async def create_lineage_edges(self, rebuild_graph: bool = True) -> Dict[str, Any]:
        """Create lineage edges between the agent and all accessed assets.
        
        Args:
            rebuild_graph: Whether to rebuild the lineage graph after creating edges
            
        Returns:
            Summary of created edges and any errors
        """
        if not self.bigeye_client:
            return {
                "success": False,
                "error": "No Bigeye client configured"
            }
            
        if not self.accessed_assets:
            return {
                "success": True,
                "message": "No assets tracked",
                "edges_created": 0
            }
            
        # Ensure agent node exists
        agent_id = await self.ensure_agent_node()
        if not agent_id:
            return {
                "success": False,
                "error": "Failed to create or find agent node"
            }
            
        edges_created = 0
        errors = []
        assets_not_in_catalog = []
        
        # Process all tracked assets
        for database, schemas in self.accessed_assets.items():
            for schema, tables in schemas.items():
                for table, columns in tables.items():
                    # If we tracked specific columns, create edges to columns
                    # Otherwise create edge to table
                    if "*" in columns:
                        # Table-level access
                        node_id = await self.find_asset_node_id(database, schema, table)
                        if node_id:
                            edge_key = (node_id, agent_id)
                            if edge_key not in self.created_edges:
                                success = await self._create_edge(node_id, agent_id, 
                                    f"{database}.{schema}.{table}", "table")
                                if success:
                                    edges_created += 1
                                    self.created_edges.add(edge_key)
                                else:
                                    errors.append(f"Failed to create edge for table {database}.{schema}.{table}")
                        else:
                            assets_not_in_catalog.append(f"{database}.{schema}.{table}")
                    else:
                        # Column-level access
                        for column in columns:
                            node_id = await self.find_asset_node_id(database, schema, table, column)
                            if node_id:
                                edge_key = (node_id, agent_id)
                                if edge_key not in self.created_edges:
                                    success = await self._create_edge(node_id, agent_id,
                                        f"{database}.{schema}.{table}.{column}", "column")
                                    if success:
                                        edges_created += 1
                                        self.created_edges.add(edge_key)
                                    else:
                                        errors.append(f"Failed to create edge for column {database}.{schema}.{table}.{column}")
                            else:
                                # Fall back to table-level if column not found
                                table_node_id = await self.find_asset_node_id(database, schema, table)
                                if table_node_id:
                                    edge_key = (table_node_id, agent_id)
                                    if edge_key not in self.created_edges:
                                        success = await self._create_edge(table_node_id, agent_id,
                                            f"{database}.{schema}.{table}", "table")
                                        if success:
                                            edges_created += 1
                                            self.created_edges.add(edge_key)
                                else:
                                    assets_not_in_catalog.append(f"{database}.{schema}.{table}.{column}")
                                    
        return {
            "success": len(errors) == 0,
            "edges_created": edges_created,
            "assets_tracked": self.get_tracked_assets(),
            "assets_not_in_catalog": assets_not_in_catalog if assets_not_in_catalog else None,
            "errors": errors if errors else None,
            "summary": f"Created {edges_created} lineage edges"
        }
        
    async def _create_edge(self, upstream_id: int, downstream_id: int, asset_name: str, asset_type: str) -> bool:
        """Create a single lineage edge.
        
        Args:
            upstream_id: Upstream node ID (data source)
            downstream_id: Downstream node ID (agent)
            asset_name: Name of the asset for logging
            asset_type: Type of asset (table/column)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.debug_print(f"Creating edge: {asset_name} ({asset_type}) -> {self.agent_name}")
            edge_result = await self.bigeye_client.create_lineage_edge(
                upstream_node_id=upstream_id,
                downstream_node_id=downstream_id,
                relationship_type="RELATIONSHIP_TYPE_LINEAGE",
                rebuild_graph=False  # We'll rebuild once at the end
            )
            
            if edge_result and not edge_result.get("error"):
                self.debug_print(f"Created edge from {asset_name} to agent")
                return True
            else:
                self.debug_print(f"Failed to create edge: {edge_result}")
                return False
                
        except Exception as e:
            self.debug_print(f"Error creating edge for {asset_name}: {str(e)}")
            return False
            
    async def cleanup_old_edges(self, retention_days: int = 30) -> Dict[str, Any]:
        """Clean up lineage edges older than the specified retention period.
        
        IMPORTANT: Only deletes edges where the agent is involved.
        
        Args:
            retention_days: Number of days to retain lineage (default: 30)
            
        Returns:
            Summary of cleanup operation
        """
        if not self.bigeye_client:
            return {
                "success": False,
                "error": "No Bigeye client configured"
            }
            
        # Ensure agent node exists
        agent_id = await self.ensure_agent_node()
        if not agent_id:
            return {
                "success": False,
                "error": "Failed to find agent node"
            }
            
        try:
            self.debug_print(f"Cleaning up edges older than {retention_days} days for agent: {self.agent_name}")
            
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            self.debug_print(f"Cutoff date: {cutoff_date.isoformat()}")
            
            # Get all edges for this agent node
            edges_result = await self.bigeye_client.get_lineage_edges_for_node(
                node_id=agent_id,
                direction="both"
            )
            
            if not edges_result or edges_result.get("error"):
                return {
                    "success": False,
                    "error": f"Failed to get edges: {edges_result}"
                }
                
            edges = edges_result.get("edges", [])
            edges_deleted = 0
            errors = []
            agent_edges_checked = 0
            
            for edge in edges:
                try:
                    # CRITICAL: Only process edges where the agent is involved
                    upstream_id = edge.get("upstream_node_id")
                    downstream_id = edge.get("downstream_node_id")
                    
                    if upstream_id != agent_id and downstream_id != agent_id:
                        self.debug_print(f"Skipping edge {edge.get('id')} - agent not involved")
                        continue
                    
                    agent_edges_checked += 1
                    
                    # Check if edge is older than retention period
                    created_at = edge.get("created_at", "")
                    if created_at:
                        edge_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        if edge_date < cutoff_date:
                            # Log what we're deleting
                            if upstream_id == agent_id:
                                self.debug_print(f"Deleting old edge: Agent -> Node {downstream_id}, created at {created_at}")
                            else:
                                self.debug_print(f"Deleting old edge: Node {upstream_id} -> Agent, created at {created_at}")
                                
                            delete_result = await self.bigeye_client.delete_lineage_edge(
                                edge_id=edge.get("id")
                            )
                            
                            if delete_result and not delete_result.get("error"):
                                edges_deleted += 1
                            else:
                                errors.append(f"Failed to delete edge {edge.get('id')}: {delete_result}")
                                
                except Exception as e:
                    errors.append(f"Error processing edge {edge.get('id')}: {str(e)}")
                    
            self.debug_print(f"Cleanup complete: deleted {edges_deleted} edges out of {agent_edges_checked} agent-related edges")
            
            return {
                "success": len(errors) == 0,
                "edges_deleted": edges_deleted,
                "agent_edges_checked": agent_edges_checked,
                "total_edges_returned": len(edges),
                "retention_days": retention_days,
                "cutoff_date": cutoff_date.isoformat(),
                "errors": errors if errors else None
            }
            
        except Exception as e:
            self.debug_print(f"Error during edge cleanup: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }