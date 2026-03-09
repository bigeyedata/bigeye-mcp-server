"""
MCP Server for Bigeye API

This server connects to the Bigeye Datawatch API and exposes resources and tools
for interacting with data quality monitoring. Credentials are provided via
environment variables from Claude Desktop configuration.
"""

from mcp.server.fastmcp import FastMCP, Context
import os
import re
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

# ---------------------------------------------------------------------------
# Metric type → column type applicability
# ---------------------------------------------------------------------------
# The API tells us which metric types belong to each dimension, but NOT which
# column data types they apply to.  This mapping encodes that heuristic.
# Key = metric type name (exact or prefix ending with *).
# Value = set of applicable type families; empty set = all types.

METRIC_TYPE_FAMILIES: Dict[str, set] = {
    # Pipeline Reliability — table-level
    "COUNT_ROWS": set(),
    "FRESHNESS": {"TIMESTAMP"},
    "FRESHNESS_DATA": {"TIMESTAMP"},
    "VOLUME": set(),
    "VOLUME_DATA": set(),
    "HOURS_SINCE_MAX_TIMESTAMP": {"TIMESTAMP"},
    "HOURS_SINCE_LAST_LOAD": set(),
    "ROWS_INSERTED": set(),
    "COUNT_READ_QUERIES": set(),
    # Completeness
    "COUNT_NULL": set(),
    "PERCENT_NULL": set(),
    "COUNT_NOT_NULL": set(),
    "PERCENT_NOT_NULL": set(),
    "COUNT_EMPTY_STRING": {"STRING"},
    "PERCENT_EMPTY_STRING": {"STRING"},
    "COUNT_NULL_AND_EMPTY": {"STRING"},
    "PERCENT_NULL_AND_EMPTY": {"STRING"},
    # Uniqueness
    "COUNT_DISTINCT": set(),
    "COUNT_DUPLICATES": set(),
    "PERCENT_DISTINCT": set(),
    "PERCENT_DUPLICATES": set(),
    # Distributions (numeric)
    "AVERAGE": {"NUMBER"},
    "MIN": {"NUMBER"},
    "MAX": {"NUMBER"},
    "SUM": {"NUMBER"},
    "MEDIAN": {"NUMBER"},
    "VARIANCE": {"NUMBER"},
    "SKEW": {"NUMBER"},
    "KURTOSIS": {"NUMBER"},
    "GEOMETRIC_MEAN": {"NUMBER"},
    "HARMONIC_MEAN": {"NUMBER"},
    "COUNT_ZERO": {"NUMBER"},
    "PERCENT_ZERO": {"NUMBER"},
    "COUNT_NEGATIVE": {"NUMBER"},
    "PERCENT_NEGATIVE": {"NUMBER"},
    "PERCENTILE_20": {"NUMBER"},
    "PERCENTILE_40": {"NUMBER"},
    "PERCENTILE_60": {"NUMBER"},
    "PERCENTILE_80": {"NUMBER"},
    "PERCENTILE_90": {"NUMBER"},
    # Validity — string metrics
    "COUNT_UUID": {"STRING"},
    "PERCENT_UUID": {"STRING"},
    "COUNT_EMAIL": {"STRING"},
    "PERCENT_EMAIL": {"STRING"},
    "PERCENT_VALUE_IN_LIST": set(),
    "COUNT_IN_LIST": set(),
    "COUNT_NOT_IN_LIST": set(),
    "PERCENT_NOT_IN_LIST": set(),
    "COUNT_MATCHING_REGEX": {"STRING"},
    "PERCENT_MATCHING_REGEX": {"STRING"},
    "COUNT_NOT_MATCHING_REGEX": {"STRING"},
    "PERCENT_NOT_MATCHING_REGEX": {"STRING"},
    # Validity — string length (prefix pattern)
    "STRING_LENGTH_*": {"STRING"},
    # Validity — timestamps
    "COUNT_INVALID_DATE": {"TIMESTAMP", "STRING"},
    "PERCENT_INVALID_DATE": {"TIMESTAMP", "STRING"},
    "COUNT_DATE_NOT_IN_FUTURE": {"TIMESTAMP"},
    "PERCENT_DATE_NOT_IN_FUTURE": {"TIMESTAMP"},
    # Custom
    "CUSTOM_SQL": set(),
}

# Pre-compute prefix patterns for _metric_applies_to_type
_PREFIX_PATTERNS = {k[:-1]: v for k, v in METRIC_TYPE_FAMILIES.items() if k.endswith("*")}

# Valid lookback types — maps user-friendly aliases to API enum values
_LOOKBACK_TYPE_MAP = {
    "DATA_TIME": "DATA_TIME_LOOKBACK_TYPE",
    "DATA_TIME_LOOKBACK_TYPE": "DATA_TIME_LOOKBACK_TYPE",
    "CLOCK_TIME": "CLOCK_TIME_LOOKBACK_TYPE",
    "CLOCK_TIME_LOOKBACK_TYPE": "CLOCK_TIME_LOOKBACK_TYPE",
    "METRIC_TIME": "METRIC_TIME_LOOKBACK_TYPE",
    "METRIC_TIME_LOOKBACK_TYPE": "METRIC_TIME_LOOKBACK_TYPE",
}

# Valid lookback interval types (only DAYS is supported for metric lookback)
_LOOKBACK_INTERVAL_TYPE_MAP = {
    "DAYS": "DAYS_TIME_INTERVAL_TYPE",
    "DAYS_TIME_INTERVAL_TYPE": "DAYS_TIME_INTERVAL_TYPE",
}

# Valid taggable entity types — maps user-friendly names to API enum values
_TAGGABLE_ENTITY_TYPE_MAP = {
    "SOURCE": "TAGGABLE_ENTITY_TYPE_SOURCE",
    "SCHEMA": "TAGGABLE_ENTITY_TYPE_SCHEMA",
    "TABLE": "TAGGABLE_ENTITY_TYPE_DATASET",
    "DATASET": "TAGGABLE_ENTITY_TYPE_DATASET",
    "METRIC": "TAGGABLE_ENTITY_TYPE_METRIC",
    "COLUMN": "TAGGABLE_ENTITY_TYPE_COLUMN",
    "DELTA": "TAGGABLE_ENTITY_TYPE_DELTA",
    "SLA": "TAGGABLE_ENTITY_TYPE_SLA",
    "CUSTOM_RULE": "TAGGABLE_ENTITY_TYPE_CUSTOM_RULE",
    # Accept full enum values directly
    "TAGGABLE_ENTITY_TYPE_SOURCE": "TAGGABLE_ENTITY_TYPE_SOURCE",
    "TAGGABLE_ENTITY_TYPE_SCHEMA": "TAGGABLE_ENTITY_TYPE_SCHEMA",
    "TAGGABLE_ENTITY_TYPE_DATASET": "TAGGABLE_ENTITY_TYPE_DATASET",
    "TAGGABLE_ENTITY_TYPE_METRIC": "TAGGABLE_ENTITY_TYPE_METRIC",
    "TAGGABLE_ENTITY_TYPE_COLUMN": "TAGGABLE_ENTITY_TYPE_COLUMN",
    "TAGGABLE_ENTITY_TYPE_DELTA": "TAGGABLE_ENTITY_TYPE_DELTA",
    "TAGGABLE_ENTITY_TYPE_SLA": "TAGGABLE_ENTITY_TYPE_SLA",
    "TAGGABLE_ENTITY_TYPE_CUSTOM_RULE": "TAGGABLE_ENTITY_TYPE_CUSTOM_RULE",
}


def _metric_applies_to_type(metric_type: str, type_family: str) -> bool:
    """Return True if *metric_type* is applicable to a column of *type_family*.

    Checks exact match first, then prefix patterns (e.g. STRING_LENGTH_*).
    If the metric type is unknown we conservatively return True.
    """
    families = METRIC_TYPE_FAMILIES.get(metric_type)
    if families is not None:
        return len(families) == 0 or type_family in families
    # Check prefix patterns
    for prefix, fam in _PREFIX_PATTERNS.items():
        if metric_type.startswith(prefix):
            return len(fam) == 0 or type_family in fam
    # Unknown metric — assume applicable
    return True


def _normalize_data_type(raw_type: str) -> str:
    """Map a raw warehouse column type to a family: TIMESTAMP, NUMBER, STRING, BOOLEAN."""
    upper = raw_type.upper().split("(")[0].strip()
    if upper in (
        "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "DATE", "DATETIME", "TIME",
    ):
        return "TIMESTAMP"
    if upper in (
        "NUMBER", "NUMERIC", "DECIMAL", "INT", "INTEGER", "BIGINT",
        "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "REAL",
    ):
        return "NUMBER"
    if upper in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    # Default: STRING (VARCHAR, CHAR, TEXT, VARIANT, etc.)
    return "STRING"


def _infer_column_role(
    column_name: str,
    data_type_family: str,
    is_primary_key: bool = False,
    is_foreign_key: bool = False,
) -> str:
    """Infer a column's semantic role from its name, type, and key flags."""
    name = column_name.upper()

    if is_primary_key:
        return "primary_key"
    if is_foreign_key:
        return "foreign_key"

    # Identifier patterns
    if name.endswith("_ID") or name == "ID":
        return "identifier"

    # Timestamp patterns
    if data_type_family == "TIMESTAMP":
        return "timestamp"

    # Boolean
    if data_type_family == "BOOLEAN" or name.startswith("IS_") or name.startswith("HAS_"):
        return "boolean"

    # Measure patterns (numeric columns that aren't IDs)
    if data_type_family == "NUMBER":
        measure_patterns = (
            "AMOUNT", "TOTAL", "PRICE", "COST", "REVENUE", "SUM",
            "COUNT", "QTY", "QUANTITY", "BALANCE", "RATE", "SCORE",
        )
        if any(pat in name for pat in measure_patterns):
            return "measure"
        return "measure"  # default for remaining numerics

    # Categorical string patterns
    if data_type_family == "STRING":
        categorical_patterns = (
            "STATUS", "TYPE", "CATEGORY", "STATE", "LEVEL", "TIER",
            "CODE", "FLAG", "MODE", "PRIORITY", "ROLE",
        )
        if any(pat in name for pat in categorical_patterns):
            return "categorical"
        # Free text patterns
        free_text_patterns = (
            "DESCRIPTION", "COMMENT", "NOTE", "ADDRESS", "MESSAGE",
            "BODY", "TEXT", "CONTENT", "MEMO", "SUMMARY",
        )
        if any(pat in name for pat in free_text_patterns):
            return "free_text"
        return "categorical"  # default for remaining strings

    return "categorical"


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
    Example: "Show issues for schema X" → Use list_issues() tool with filters

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

    KNOWLEDGEBASE SERVER
    ====================
    If a bigeye-knowledgebase MCP server is also available, prefer its search tools for
    discovering data assets:
    - search_metadata: Semantic search across tables, columns, schemas, BI reports
    - list_schemas, list_tables, list_sources: Browse the data catalog
    - list_integrations, list_catalog_entities: Find BI dashboards and reports

    Use THIS server for everything else: quality reports, monitoring, issues, lineage
    analysis, actions, agent tracking, profiling, and dimension management.

    If the knowledgebase tools are not available, fall back to this server's search tools
    (search_tables, search_columns, search_schemas).

    IMPORTANT: Understanding Issue and Incident References
    =======================================================
    Issues and incidents have TWO different identifiers - understanding the distinction is CRITICAL:

    1. 'id' field - Internal database ID (e.g., 12345, 67890)
       - This is the internal system identifier
       - Users typically DON'T know this number
       - Used for API operations like create_incident(), update_issue(), etc.
       - DO NOT use this when users reference an issue by number

    2. 'name' field - Display name/reference (e.g., "10921", "data-quality-alert")
       - This is what users see in the Bigeye UI
       - This is what users will reference in conversations
       - When a user says "incident 10921" or "issue 10921", they mean THIS field

    WORKFLOW for Issue/Incident References:
    ----------------------------------------
    When a user mentions an issue or incident by number or name:

    1. ALWAYS use search_issues() to find the issue
       Example: User says "Show me incident 10921"
       → Use search_issues(name_query="10921")

    2. DO NOT try to use the number as an 'id' parameter in other tools
       ❌ WRONG: list_related_issues(starting_issue_id=10921)
       ✓ CORRECT: search_issues(name_query="10921") first,
                  then use the returned 'id' field if needed

    3. Present search results if multiple matches are found
       - Show the issue name, status, description, and affected tables
       - Ask user to confirm if needed

    4. Only after finding the correct issue, use its 'id' field for other operations
       - The 'id' from the search result can be used with list_related_issues()
       - The 'id' from the search result can be used with update_issue()
       - The 'id' from the search result can be used with create_incident()

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


async def _resolve_table(
    client: BigeyeAPIClient,
    workspace_id: int,
    table_name: str,
    schema_name: Optional[str] = None,
    include_columns: bool = False,
) -> Dict[str, Any]:
    """Resolve a table by name, handling case-insensitivity and the DATABASE.SCHEMA quirk.

    Bigeye stores schema names as DATABASE.SCHEMA (e.g. TOOY_DEMO_V2.PROD_REPL).
    Callers often pass just the schema portion (e.g. "prod_repl") or use the wrong
    case.  This helper tries:
      1. Exact search with the provided schema_name (uppercased).
      2. If no results and schema_name has no dot, retry without the schema filter
         and match client-side using a suffix match on schemaName.
      3. If still no results, retry with just the table name (no schema filter).

    Returns the first matching table dict, or an error dict with 'error' key.
    """
    uc_table = table_name.upper()
    uc_schema = schema_name.upper() if schema_name else None

    # Attempt 1: search with both filters
    result = await client.search_tables(
        workspace_id=workspace_id,
        table_name=uc_table,
        schema_names=[uc_schema] if uc_schema else None,
        include_columns=include_columns,
    )
    tables = result.get("tables", [])

    # Attempt 2: if schema was provided but has no dot, it's probably just the
    # schema portion — retry without schema filter and match client-side.
    if not tables and uc_schema and "." not in uc_schema:
        debug_print(
            f"No results for table={uc_table} schema={uc_schema}; "
            "retrying without schema filter (suffix match)"
        )
        result = await client.search_tables(
            workspace_id=workspace_id,
            table_name=uc_table,
            include_columns=include_columns,
        )
        tables = [
            t for t in result.get("tables", [])
            if (t.get("schemaName") or "").upper().endswith(uc_schema)
        ]

    # Attempt 3: last resort — just table name, no schema
    if not tables and uc_schema:
        debug_print(
            f"Still no results; retrying with just table_name={uc_table}"
        )
        result = await client.search_tables(
            workspace_id=workspace_id,
            table_name=uc_table,
            include_columns=include_columns,
        )
        tables = result.get("tables", [])

    if not tables:
        return {"error": True, "message": f"Table '{table_name}' not found in Bigeye catalog"}

    table = tables[0]
    if not table.get("id"):
        return {"error": True, "message": f"Table '{table_name}' found but has no ID"}

    return table


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
async def get_health_status() -> Dict[str, Any]:
    """Check the health and connectivity of the Bigeye API. Returns API status and version."""

    client = get_api_client()
    debug_print("Checking API health")
    result = await client.check_health()
    debug_print(f"Health check result: {result}")
    return result

@mcp.tool()
async def list_issues(
    statuses: Optional[List[str]] = None,
    schema_names: Optional[List[str]] = None,
    page_size: Optional[int] = None,
    page_cursor: Optional[str] = None,
    compact: bool = True,
    max_issues: Optional[int] = 15
) -> Dict[str, Any]:
    """List data quality issues across the workspace. Supports filtering by status (ISSUE_STATUS_NEW, ISSUE_STATUS_ACKNOWLEDGED, ISSUE_STATUS_CLOSED, ISSUE_STATUS_MONITORING, ISSUE_STATUS_MERGED) and schema. Returns compact summaries by default. Best for broad views like 'show all open issues'. For issues on a specific table, use list_table_issues instead.

    Args:
        statuses: Optional list of issue statuses to filter by. Possible values:
            - ISSUE_STATUS_NEW
            - ISSUE_STATUS_ACKNOWLEDGED
            - ISSUE_STATUS_CLOSED
            - ISSUE_STATUS_MONITORING
            - ISSUE_STATUS_MERGED
        schema_names: Optional list of schema names to filter issues by
        page_size: Optional number of issues to request from API per page (default: 20)
        page_cursor: Cursor for pagination
        compact: If True (default), returns only minimal fields (id, name, status, table, schema).
                If False, returns standard fields including description and metric info.
                Use compact=True to list issues, then get_issue() for specifics.
        max_issues: Maximum number of issues to return (default: 15). Prevents context overload.
                   Set to None to return all issues (use with caution).
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
    debug_print(f"compact={compact}, max_issues={max_issues}")

    if statuses:
        debug_print(f"Filtering by statuses: {statuses}")
    if schema_names:
        debug_print(f"Filtering by schema names: {schema_names}")

    result = await client.fetch_issues(
        workspace_id=workspace_id,
        currentStatus=statuses,
        schemaNames=schema_names,
        page_size=page_size if page_size else 20,
        page_cursor=page_cursor,
        include_full_history=False,
        compact=compact,
        max_issues=max_issues
    )

    issue_count = len(result.get("issues", []))
    debug_print(f"Found {issue_count} issues")

    return result


@mcp.tool()
async def get_issue(
    issue_id: int
) -> Dict[str, Any]:
    """Get full details for a single issue by its internal ID (not display name). Returns event history, metric details, and metadata. To find an issue's internal ID from its display name, use search_issues first.

    Args:
        issue_id: The internal database ID of the issue (from the 'id' field in issue lists)
    """

    client = get_api_client()

    debug_print(f"Fetching details for issue ID: {issue_id}")

    result = await client.fetch_single_issue(issue_id=issue_id)

    return result

@mcp.tool()
async def search_issues(
    name_query: str,
    statuses: Optional[List[str]] = None,
    exact_match: bool = False
) -> Dict[str, Any]:
    """Find issues or incidents by their display name — the number shown in the Bigeye UI (e.g. '10921'). ALWAYS use this when a user references an issue by number. Returns matching issues with their internal IDs for use with get_issue, update_issue, and other tools.

    Args:
        name_query: The issue name or partial name to search for (e.g., "10921", "data-quality")
        statuses: Optional list of issue statuses to filter by. Possible values:
            - ISSUE_STATUS_NEW
            - ISSUE_STATUS_ACKNOWLEDGED
            - ISSUE_STATUS_CLOSED
            - ISSUE_STATUS_MONITORING
            - ISSUE_STATUS_MERGED
        exact_match: If True, only return exact name matches. If False (default),
                    returns partial matches (case-insensitive)
    """

    client = get_api_client()
    workspace_id = config.get('workspace_id')

    # Safety check
    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration'
        }

    debug_print(f"Searching for issues with name: {name_query}")
    debug_print(f"Exact match: {exact_match}")

    if statuses:
        debug_print(f"Filtering by statuses: {statuses}")

    result = await client.search_issues_by_name(
        workspace_id=workspace_id,
        name_query=name_query,
        statuses=statuses,
        exact_match=exact_match
    )

    match_count = len(result.get("issues", []))
    debug_print(f"Found {match_count} issues matching '{name_query}'")

    return result

@mcp.tool()
async def list_related_issues(
    starting_issue_id: int
) -> Dict[str, Any]:
    """List issues related to a given issue via upstream/downstream lineage. Returns related issues with root cause flags (isRootCause=true). Requires internal issue ID.

    Args:
        starting_issue_id: The internal ID of the issue to find related issues for
    """
    
    client = get_api_client()
    
    debug_print(f"Fetching issues for starting issue {starting_issue_id}")
        
    return await client.fetch_related_issues(starting_issue_id)

@mcp.tool()
async def list_table_issues(
    table_name: str,
    warehouse_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    statuses: Optional[List[str]] = None
) -> Dict[str, Any]:
    """List data quality issues for a specific table by name. Best for investigating a known table. For workspace-wide issues use list_issues. Requires table_name.

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
async def create_incident(
    issue_ids: List[int],
    existing_incident_id: Optional[int] = None,
    incident_name: Optional[str] = None
) -> Dict[str, Any]:
    """Create an incident by merging related issues, or add issues to an existing incident. Requires at least 2 issue IDs for new incidents, or 1 issue ID plus an existing_incident_id.

    Args:
        issue_ids: List of issue IDs to merge (at least 2 for new incident, at least 1 for existing)
        existing_incident_id: Optional ID of an existing incident to merge issues into.
                             If not provided, a new incident will be created.
        incident_name: Optional name for the incident
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
async def get_resolution_steps(
    issue_id: int
) -> Dict[str, Any]:
    """Get recommended resolution steps for an issue. Returns step-by-step guidance for remediation. Requires internal issue ID.

    Args:
        issue_id: The internal ID of the issue to get resolution steps for
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
    """Update an issue's status, priority, or add a timeline message. Valid statuses: ISSUE_STATUS_NEW, ISSUE_STATUS_ACKNOWLEDGED, ISSUE_STATUS_CLOSED, ISSUE_STATUS_MONITORING, ISSUE_STATUS_MERGED. When closing, requires a closing_label (METRIC_RUN_LABEL_TRUE_NEGATIVE, METRIC_RUN_LABEL_FALSE_POSITIVE, etc.).

    Args:
        issue_id: The ID of the issue to update
        new_status: New status for the issue
        closing_label: Required when new_status is ISSUE_STATUS_CLOSED
        priority: New priority (ISSUE_PRIORITY_LOW, ISSUE_PRIORITY_MED, ISSUE_PRIORITY_HIGH)
        message: Timeline message to add to the issue
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
async def delete_incident_members(
    issue_ids: Optional[List[int]] = None,
    parent_issue_ids: Optional[List[int]] = None,
    assignee_id: Optional[int] = None,
    new_status: Optional[str] = None
) -> Dict[str, Any]:
    """Remove issues from an incident. Specify individual issue_ids to unmerge, or parent_issue_ids to unmerge all children from an incident.

    Args:
        issue_ids: Optional list of specific issue IDs to unmerge from their incidents
        parent_issue_ids: Optional list of incident IDs to unmerge all child issues from
        assignee_id: Optional user ID to assign the unmerged issues to
        new_status: Optional new status for the unmerged issues
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
async def list_table_metrics(
    table_name: str,
    schema_name: Optional[str] = None
) -> Dict[str, Any]:
    """List all metrics (monitors) configured on a table from the live Bigeye API. Returns full metric configurations including schedules, thresholds, lookback windows, and which data dimension each metric belongs to. For a quick coverage gap analysis, use get_table_dimension_coverage instead.

    Args:
        table_name: Table name to get metrics for
        schema_name: Optional schema name to narrow the search
    """
    client = get_api_client()
    workspace_id = config.get('workspace_id')

    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration'
        }

    debug_print(f"Fetching metrics for table {table_name}")

    try:
        # Resolve table name → table ID (GET /api/v1/metrics requires tableIds, not tableName)
        table = await _resolve_table(client, workspace_id, table_name, schema_name)
        if table.get("error"):
            return table

        table_id = table["id"]

        result = await client.get_table_metrics(
            workspace_id=workspace_id,
            table_ids=[table_id],
        )

        # Enrich metrics with dimension details from taxonomy
        try:
            dims_result = await client.make_request("/api/v1/dimensions")
            if isinstance(dims_result, list):
                dims_result = {"dimensions": dims_result}
            # Build lookup by dimension ID
            dim_by_id = {}
            for dim in dims_result.get("dimensions", []):
                dim_id = dim.get("id")
                if dim_id is not None:
                    dim_by_id[dim_id] = {
                        "dimension_id": dim_id,
                        "dimension_name": dim.get("name"),
                        "dimension_category": dim.get("topLevelCategory", ""),
                    }

            metrics = result.get("metrics", [])
            for m in metrics:
                # The API returns dimension.id natively on each metric
                native_dim = m.get("dimension") or {}
                dim_id = native_dim.get("id")
                if dim_id and dim_id in dim_by_id:
                    m["dimension"] = dim_by_id[dim_id]

            covered_dims = {m["dimension"]["dimension_name"] for m in metrics if isinstance(m.get("dimension"), dict) and m["dimension"].get("dimension_name")}
            result["dimension_summary"] = {
                "covered_dimensions": sorted(covered_dims),
                "metric_count": len(metrics),
            }
        except Exception:
            # Don't fail the whole response if dimension enrichment fails
            pass

        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching table metrics: {str(e)}"
        }

@mcp.tool()
async def list_data_sources() -> Dict[str, Any]:
    """List all data sources (warehouses) connected to Bigeye. Returns source names, types (SNOWFLAKE, DATABRICKS, etc.), and connection details. Prefer list_sources (knowledgebase) for faster cached results."""
    client = get_api_client()
    workspace_id = config.get('workspace_id')

    if not workspace_id:
        return {
            'error': 'Workspace ID not configured',
            'hint': 'Check your Claude Desktop configuration'
        }

    debug_print("Fetching data sources")

    try:
        result = await client.get_sources(workspace_id=workspace_id)
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching data sources: {str(e)}"
        }

@mcp.tool()
async def get_lineage_graph(
    node_id: int,
    direction: str = "bidirectional",
    max_depth: Optional[int] = None,
    include_issues: bool = True
) -> Dict[str, Any]:
    """Get the full lineage graph (upstream/downstream/both) from a starting node. Returns nodes and edges showing data flow. Requires node_id from search_lineage_nodes.

    Args:
        node_id: The ID of the lineage node to get the graph for
        direction: Direction to traverse: "bidirectional" (default), "upstream", or "downstream"
        max_depth: Maximum depth to traverse (optional)
        include_issues: Whether to include issue counts for each node (default: True)
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
async def get_lineage_node(
    node_id: int
) -> Dict[str, Any]:
    """Get details for a specific lineage node (type, name, properties). Requires node_id.

    Args:
        node_id: The ID of the lineage node to get details for
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
async def list_lineage_node_issues(
    node_id: int
) -> Dict[str, Any]:
    """List issues for a lineage node by its node_id. Best for when you already have a node_id from search_lineage_nodes. If you only have a table name, use list_table_issues instead.

    Args:
        node_id: The ID of the lineage node to get issues for
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
async def get_upstream_root_causes(
    node_id: int,
    max_depth: Optional[int] = 5
) -> Dict[str, Any]:
    """Analyze upstream lineage to identify root causes of data quality issues. Traverses upstream from a node to find the origin of problems. Requires node_id.

    Args:
        node_id: The ID of the lineage node where issues are occurring
        max_depth: Maximum depth to search upstream (default: 5)
    """
    
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
async def get_downstream_impact(
    node_id: int,
    max_depth: Optional[int] = 5,
    include_integration_entities: bool = True,
    impact_focus: Optional[str] = "all"
) -> Dict[str, Any]:
    """Analyze downstream impact of issues at a lineage node, categorized by type (analytics, data products, critical). Returns severity assessment and stakeholder notifications. Requires node_id.

    Args:
        node_id: The ID of the lineage node with potential data quality issues
        max_depth: Maximum depth to search downstream (default: 5)
        include_integration_entities: Include BI tools, dashboards, etc. (default: True)
        impact_focus: Type of impact to focus on: "all" (default), "analytics", "data_products", or "critical"
    """
    
    debug_print(f"Analyzing downstream impact for node {node_id} with focus: {impact_focus}")
    
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
async def get_issue_lineage_trace(
    issue_id: int,
    include_root_cause_analysis: bool = True,
    include_impact_analysis: bool = True,
    max_depth: Optional[int] = 5
) -> Dict[str, Any]:
    """Trace a data quality issue end-to-end through lineage: upstream root causes to downstream impact. Requires internal issue_id (from search_issues, not the display name).

    Args:
        issue_id: The internal ID of the issue to trace through lineage
        include_root_cause_analysis: Whether to perform upstream root cause analysis (default: True)
        include_impact_analysis: Whether to perform downstream impact analysis (default: True)
        max_depth: Maximum depth for lineage traversal (default: 5)
    """
    
    debug_print(f"Tracing lineage path for issue {issue_id}")
    
    try:
        # First get issues to find the specific one
        issues_response = await list_issues(page_size=1000)
        
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
            root_cause_result = await get_upstream_root_causes(
                node_id=lineage_node_id,
                max_depth=max_depth
            )
            result["root_cause_analysis"] = root_cause_result
        
        # Perform impact analysis if requested  
        if include_impact_analysis:
            debug_print("Performing impact analysis")
            impact_result = await get_downstream_impact(
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
    
    root_cause_analysis = await get_upstream_root_causes(
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
async def search_lineage_nodes(
    workspace_id: Optional[int] = None,
    search_string: str = "*",
    node_type: Optional[str] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """Find lineage node IDs by path pattern (e.g. 'WAREHOUSE/SCHEMA/TABLE'). Supports wildcards ('*/*/ORDERS'). Best for: getting a node_id required by get_lineage_graph, get_downstream_impact, and other lineage tools. Not for general table discovery — use search_tables for that.

    Args:
        workspace_id: Optional Bigeye workspace ID. If not provided, uses the configured workspace.
        search_string: Search string using path format (e.g. 'WAREHOUSE/SCHEMA/TABLE') or wildcards
        node_type: Optional node type filter: "DATA_NODE_TYPE_TABLE", "DATA_NODE_TYPE_COLUMN", "DATA_NODE_TYPE_CUSTOM"
        limit: Maximum number of results to return (default: 20)
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
    debug_print(f"=== search_lineage_nodes called ===")
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
    """Explore tables in Bigeye's catalog. For browsing the data catalog, prefer list_tables + list_schemas (knowledgebase).

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
    """Search for schemas by name (supports partial matching). Returns matching schemas with table counts. Optionally filter by warehouse. For browsing schemas with table counts, prefer list_schemas (knowledgebase).

    Args:
        schema_name: Optional schema name to search for (supports partial matching)
        warehouse_names: Optional list of warehouse names to filter by
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
    """Search the Bigeye catalog for tables by exact or partial name match. Best for: finding a specific table when you know its name. Returns full qualified names, row counts, column counts. For natural language/conceptual queries, use search_metadata (knowledgebase) instead. After resolving a table, use list_table_metrics for monitor details or get_table_dimension_coverage for coverage gap analysis.

    Args:
        table_name: Optional table name to search for (supports partial matching)
        schema_names: Optional list of schema names to filter by
        warehouse_names: Optional list of warehouse names to filter by
        include_columns: Whether to include column information in the response
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
async def list_report_upstream_issues(
    report_id: int
) -> Dict[str, Any]:
    """List upstream data quality issues affecting a BI report or dashboard. Requires the report's lineage node_id. To discover reports/dashboards first, use list_catalog_entities (knowledgebase).

    Args:
        report_id: The lineage node ID of the BI report/dashboard
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
async def get_table_profile(
    table_id: int
) -> Dict[str, Any]:
    """Get the data profile report for a table including column statistics, data distribution, and profile history. Requires table_id.

    Args:
        table_id: The ID of the table to get the profile for
    """

    client = get_api_client()
    debug_print(f"Getting profile for table {table_id}")

    try:
        return await client.get_profile_for_table(table_id=table_id)

    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting profile for table {table_id}: {str(e)}"
        }

@mcp.tool()
async def create_profile_job(
    table_id: int
) -> Dict[str, Any]:
    """Queue a new data profiling job for a table. Returns a workflow ID to track progress with get_profile_job_status. Requires table_id.

    Args:
        table_id: The ID of the table to queue profiling for
    """
    client = get_api_client()
    debug_print(f"Queuing profile job for table {table_id}")

    try:
        return await client.queue_table_profile(table_id=table_id)
    except Exception as e:
        return {
            "error": True,
            "message": f"Error queuing profile for table {table_id}: {str(e)}"
        }

@mcp.tool()
async def get_profile_job_status(
    table_id: int
) -> Dict[str, Any]:
    """Check the status of a profiling job (queued, running, completed). Requires table_id.

    Args:
        table_id: The ID of the table to check profiling workflow status for
    """
    client = get_api_client()
    debug_print(f"Getting profile workflow status for table {table_id}")

    try:
        return await client.get_profile_workflow_status_for_table(table_id=table_id)
    except Exception as e:
        return {
            "error": True,
            "message": f"Error getting profile workflow status for table {table_id}: {str(e)}"
        }

@mcp.tool()
async def search_columns(
    column_name: Optional[str] = None,
    table_names: Optional[List[str]] = None,
    schema_names: Optional[List[str]] = None,
    warehouse_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Search for columns by name (supports partial matching). Returns matching columns with full qualified names. Optionally filter by table, schema, or warehouse. For semantic/conceptual column discovery, prefer search_metadata (knowledgebase).

    Args:
        column_name: Optional column name to search for (supports partial matching)
        table_names: Optional list of table names to filter by
        schema_names: Optional list of schema names to filter by
        warehouse_names: Optional list of warehouse names to filter by
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

@mcp.tool()
async def list_dimensions() -> Dict[str, Any]:
    """List all data-quality dimensions with their metric type mappings. Returns each dimension's name, top-level category (PIPELINE_RELIABILITY or DATA_QUALITY), and the metric types that belong to it. For a table-specific coverage analysis that maps dimensions to existing monitors, use get_table_dimension_coverage."""
    client = get_api_client()

    debug_print("Fetching all dimensions")

    try:
        result = await client.make_request("/api/v1/dimensions")
        if isinstance(result, list):
            result = {"dimensions": result}

        dimensions = result.get("dimensions", [])

        formatted = []
        for dim in dimensions:
            # Extract metric type names from instanceTypesWithCounts
            # The metric type name is in the "name" field, not "instanceType"
            # (instanceType is always "DIMENSION_INSTANCE_TYPE_METRIC")
            metric_types = [
                entry.get("name", "")
                for entry in dim.get("instanceTypesWithCounts", [])
                if entry.get("name")
            ]

            formatted.append({
                "id": dim.get("id"),
                "name": dim.get("name"),
                "description": dim.get("description"),
                "category": dim.get("topLevelCategory", ""),
                "metric_types": metric_types,
            })

        return {
            "total_dimensions": len(formatted),
            "dimensions": formatted,
        }
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching dimensions: {str(e)}",
        }


@mcp.tool()
async def get_dimension(
    dimension_id: int,
) -> Dict[str, Any]:
    """Get full details for a single Data Dimension by its ID. Returns the dimension's name, description, and entity metadata.

    Args:
        dimension_id: The ID of the dimension to retrieve
    """
    client = get_api_client()

    debug_print(f"Fetching dimension {dimension_id}")

    try:
        result = await client.make_request(f"/api/v1/dimension/{dimension_id}")
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching dimension {dimension_id}: {str(e)}",
        }


@mcp.tool()
async def create_dimension(
    name: str,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new Data Dimension in the Bigeye workspace.

    Args:
        name: The name for the new dimension
        description: Optional description for the dimension
    """
    client = get_api_client()

    debug_print(f"Creating dimension: name={name}")

    body: Dict[str, Any] = {"name": name}
    if description is not None:
        body["description"] = description

    try:
        result = await client.make_request(
            "/api/v1/dimensions",
            method="POST",
            json_data=body,
        )
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error creating dimension: {str(e)}",
        }


@mcp.tool()
async def update_dimension(
    dimension_id: int,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Update an existing Data Dimension's name or description.

    Args:
        dimension_id: The ID of the dimension to update
        name: New name for the dimension (optional)
        description: New description for the dimension (optional)
    """
    client = get_api_client()

    debug_print(f"Updating dimension {dimension_id}")

    body: Dict[str, Any] = {}
    if name is not None:
        body["name"] = name
    if description is not None:
        body["description"] = description

    if not body:
        return {
            "error": True,
            "message": "No fields to update. Provide at least one of: name, description",
        }

    try:
        result = await client.make_request(
            f"/api/v1/dimensions/{dimension_id}",
            method="PUT",
            json_data=body,
        )
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error updating dimension {dimension_id}: {str(e)}",
        }


@mcp.tool()
async def delete_dimension(
    dimension_id: int,
) -> Dict[str, Any]:
    """Delete a Data Dimension by its ID. This action is permanent.

    Args:
        dimension_id: The ID of the dimension to delete
    """
    client = get_api_client()

    debug_print(f"Deleting dimension {dimension_id}")

    try:
        result = await client.make_request(
            f"/api/v1/dimensions/{dimension_id}",
            method="DELETE",
        )
        return {
            "success": True,
            "message": f"Dimension {dimension_id} deleted",
            "response": result,
        }
    except Exception as e:
        return {
            "error": True,
            "message": f"Error deleting dimension {dimension_id}: {str(e)}",
        }


@mcp.tool()
async def list_table_level_metrics() -> Dict[str, Any]:
    """List metric types that are table-level (not column-level). Returns metric names like FRESHNESS, VOLUME, COUNT_ROWS, etc. Use this to distinguish table-level from column-level metrics when analyzing coverage."""
    client = get_api_client()

    debug_print("Fetching table-level metric names")

    try:
        result = await client.get_table_level_metric_names()
        return result
    except Exception as e:
        return {
            "error": True,
            "message": f"Error fetching table-level metric names: {str(e)}",
        }


@mcp.tool()
async def create_metric(
    table_name: str,
    metric_type: str,
    column_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    table_id: Optional[int] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    lookback_type: Optional[str] = None,
    lookback_interval_type: Optional[str] = None,
    lookback_interval_value: Optional[int] = None,
    filters: Optional[List[str]] = None,
    group_bys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create a new metric (monitor) on a table. Supports predefined metric types like COUNT_ROWS, PERCENT_NULL, FRESHNESS, etc.

    Column-level metrics (e.g. PERCENT_NULL, COUNT_DISTINCT) require column_name.
    Table-level metrics (e.g. COUNT_ROWS, FRESHNESS) must NOT have column_name.

    Prefer passing table_id (from search_tables) for reliable lookups. Falls back to name-based search if table_id is not provided.

    Args:
        table_name: Name of the table to create the metric on
        metric_type: Predefined metric type (e.g. COUNT_ROWS, PERCENT_NULL, FRESHNESS)
        column_name: Column to monitor (required for column-level metrics, omit for table-level)
        schema_name: Optional schema name to disambiguate table search
        table_id: Table ID from search_tables — preferred, avoids ambiguous name lookups
        name: Optional display name for the metric
        description: Optional description for the metric
        lookback_type: Optional lookback type — DATA_TIME, CLOCK_TIME, or METRIC_TIME
        lookback_interval_type: Optional interval type — only DAYS is supported for metric lookback
        lookback_interval_value: Optional interval value (e.g. 7)
        filters: Optional list of SQL filter expressions
        group_bys: Optional list of column names to group by
    """
    client = get_api_client()
    workspace_id = config.get("workspace_id")

    # 1. Check workspace_id
    if not workspace_id:
        return {
            "error": "Workspace ID not configured",
            "hint": "Check your Claude Desktop configuration for BIGEYE_WORKSPACE_ID",
        }

    # 2. Validate metric_type
    metric_upper = metric_type.upper()
    known = metric_upper in METRIC_TYPE_FAMILIES
    if not known:
        # Check prefix patterns
        for prefix in _PREFIX_PATTERNS:
            if metric_upper.startswith(prefix):
                known = True
                break
    if not known:
        return {
            "error": f"Unknown metric type: {metric_type}",
            "known_types": sorted(METRIC_TYPE_FAMILIES.keys()),
            "hint": "Use one of the known metric types listed above",
        }

    # Validate and map lookback_type enum
    if lookback_type:
        lt_upper = lookback_type.upper()
        if lt_upper not in _LOOKBACK_TYPE_MAP:
            return {
                "error": f"Invalid lookback_type: {lookback_type}",
                "valid_values": sorted(set(_LOOKBACK_TYPE_MAP.values())),
                "hint": "Use DATA_TIME, CLOCK_TIME, or METRIC_TIME",
            }
        lookback_type = _LOOKBACK_TYPE_MAP[lt_upper]

    # Validate and map lookback_interval_type enum
    if lookback_interval_type:
        lit_upper = lookback_interval_type.upper()
        if lit_upper not in _LOOKBACK_INTERVAL_TYPE_MAP:
            return {
                "error": f"Invalid lookback_interval_type: {lookback_interval_type}",
                "valid_values": sorted(set(_LOOKBACK_INTERVAL_TYPE_MAP.values())),
                "hint": "Only DAYS (DAYS_TIME_INTERVAL_TYPE) is supported for metric lookback",
            }
        lookback_interval_type = _LOOKBACK_INTERVAL_TYPE_MAP[lit_upper]

    # 3. Determine if metric is table-level or column-level
    try:
        table_level_result = await client.get_table_level_metric_names()
        table_level_names: set = set()
        if isinstance(table_level_result, dict):
            for key in ("metricNames", "metric_names", "names"):
                if key in table_level_result:
                    table_level_names = set(table_level_result[key])
                    break
        elif isinstance(table_level_result, list):
            table_level_names = set(table_level_result)
    except Exception:
        table_level_names = set()

    is_table_level = metric_upper in table_level_names

    # 4. Column-level metric without column_name
    if not is_table_level and not column_name:
        return {
            "error": f"Metric type '{metric_upper}' is column-level and requires a column_name",
            "hint": "Provide the column_name parameter",
        }

    # 5. Table-level metric with column_name
    if is_table_level and column_name:
        return {
            "error": f"Metric type '{metric_upper}' is table-level and does not use a column_name",
            "hint": "Remove the column_name parameter",
        }

    # 6. Resolve table
    debug_print(f"Creating metric {metric_upper} on table {table_name or table_id}")

    try:
        if table_id:
            table_result = await client.fetch_tables(
                workspace_id=workspace_id,
                table_ids=[table_id],
                include_columns=True,
            )
        else:
            table_result = await client.search_tables(
                workspace_id=workspace_id,
                table_name=table_name,
                schema_names=[schema_name] if schema_name else None,
                include_columns=True,
            )
        tables = table_result.get("tables", []) if isinstance(table_result, dict) else []
    except Exception as e:
        return {"error": f"Error searching for table: {str(e)}"}

    if not tables:
        identifier = f"ID {table_id}" if table_id else f"'{table_name}'"
        return {"error": f"Table {identifier} not found"}

    # 7. Multiple tables — disambiguate
    if len(tables) > 1 and not table_id:
        return {
            "error": "Multiple tables match — please specify table_id or schema_name to disambiguate",
            "matching_tables": [
                {
                    "id": t.get("id"),
                    "name": t.get("name"),
                    "schema": t.get("schemaName"),
                    "database": t.get("databaseName"),
                    "warehouse": t.get("warehouseName"),
                }
                for t in tables
            ],
        }

    table = tables[0]
    resolved_table_id = table.get("id")
    warehouse_id = table.get("warehouseId")
    columns = table.get("columns", [])

    # 8. Extract dataset_id + warehouse_id
    if not resolved_table_id or not warehouse_id:
        return {"error": f"Table '{table_name}' found but missing id or warehouseId"}

    # 9. Validate column exists (for column-level metrics)
    if column_name:
        matched_col = None
        for col in columns:
            if col.get("name", "").lower() == column_name.lower():
                matched_col = col
                break
        if not matched_col:
            available = sorted(col.get("name", "") for col in columns)
            return {
                "error": f"Column '{column_name}' not found on table '{table.get('name', '')}'",
                "available_columns": available,
            }

        # 10. Check column type compatibility
        raw_type = matched_col.get("dataType", "")
        type_family = _normalize_data_type(raw_type)
        if not _metric_applies_to_type(metric_upper, type_family):
            required_families = METRIC_TYPE_FAMILIES.get(metric_upper, set())
            if not required_families:
                for prefix, fam in _PREFIX_PATTERNS.items():
                    if metric_upper.startswith(prefix):
                        required_families = fam
                        break
            return {
                "error": f"Metric '{metric_upper}' does not apply to column '{column_name}' (type: {raw_type}, family: {type_family})",
                "required_type_families": sorted(required_families),
            }
        # Use the exact column name from the API (preserves casing)
        column_name = matched_col.get("name", column_name)

    # For table-level metrics the API still requires a parameters entry
    # with a column reference.  Pick the first column from the table.
    if is_table_level and not column_name and columns:
        column_name = columns[0].get("name")

    # All validations passed — create the metric
    try:
        result = await client.create_metric(
            warehouse_id=warehouse_id,
            dataset_id=resolved_table_id,
            metric_name=metric_upper,
            column_name=column_name,
            name=name,
            description=description,
            lookback_type=lookback_type,
            lookback_interval_type=lookback_interval_type,
            lookback_interval_value=lookback_interval_value,
            filters=filters,
            group_bys=group_bys,
        )
    except Exception as e:
        return {"error": f"Error creating metric: {str(e)}"}

    # Check for API error response (make_request returns error dict for HTTP 400+)
    if isinstance(result, dict) and result.get("error"):
        return {
            "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
        }

    # Extract metric ID from response
    metric_id = None
    if isinstance(result, dict):
        metric_id = result.get("id") or result.get("metricId")
        # Some responses nest under a "metric" key
        if not metric_id and "metric" in result:
            metric_id = result["metric"].get("id")

    return {
        "success": True,
        "message": f"Created {metric_upper} metric on table '{table.get('name', '')}'{'.' + column_name if column_name else ''}",
        "metric_id": metric_id,
        "metric": result,
    }


async def _compute_dimension_coverage(
    table_name: str,
    schema_name: Optional[str] = None,
    column_names: Optional[List[str]] = None,
    table_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Shared implementation for dimension coverage tools.

    If column_names is provided, filters the per-column results to only those
    columns (case-insensitive). Table-level coverage is always included.

    If table_id is provided, fetches the table directly by ID (avoids
    ambiguous name-based lookups). Otherwise falls back to name search.
    """
    client = get_api_client()
    workspace_id = config.get("workspace_id")

    if not workspace_id:
        return {
            "error": "Workspace ID not configured",
            "hint": "Check your Claude Desktop configuration",
        }

    debug_print(f"Computing dimension coverage for table {table_name or table_id}")

    try:
        # 1. Fetch dimensions from API → build metric→dimension mapping
        dims_result = await client.make_request("/api/v1/dimensions")
        if isinstance(dims_result, list):
            dims_result = {"dimensions": dims_result}
        api_dimensions = dims_result.get("dimensions", [])

        metric_to_dimension: Dict[str, str] = {}
        dimension_info: Dict[str, Dict[str, Any]] = {}
        for dim in api_dimensions:
            dim_name = dim.get("name", "")
            category = dim.get("topLevelCategory", "")
            # The metric type name is in the "name" field of each entry,
            # not "instanceType" (which is always "DIMENSION_INSTANCE_TYPE_METRIC")
            metric_types = [
                entry.get("name", "")
                for entry in dim.get("instanceTypesWithCounts", [])
                if entry.get("name")
            ]
            dimension_info[dim_name] = {
                "category": category,
                "metric_types": metric_types,
            }
            for mt in metric_types:
                metric_to_dimension[mt] = dim_name

        # 2. Fetch table-level metric names
        table_level_result = await client.get_table_level_metric_names()
        table_level_names = set()
        # Handle different response formats
        if isinstance(table_level_result, dict):
            for key in ("metricNames", "metric_names", "names"):
                if key in table_level_result:
                    table_level_names = set(table_level_result[key])
                    break
        elif isinstance(table_level_result, list):
            table_level_names = set(table_level_result)

        # 3. Fetch column metadata — by ID if available, otherwise name search
        if table_id:
            table_result = await client.fetch_tables(
                workspace_id=workspace_id,
                table_ids=[table_id],
                include_columns=True,
            )
            tables = table_result.get("tables", []) if isinstance(table_result, dict) else []
            if not tables:
                return {"error": f"Table ID {table_id} not found"}
            table = tables[0]
        else:
            table = await _resolve_table(
                client, workspace_id, table_name, schema_name, include_columns=True,
            )
            if table.get("error"):
                return table

        columns = table.get("columns", [])

        # If column_names filter is provided, validate and filter
        if column_names is not None:
            filter_set = {c.lower() for c in column_names}
            all_col_names = {col.get("name", "").lower() for col in columns}
            missing = filter_set - all_col_names
            if missing:
                return {
                    "error": f"Columns not found on table '{table_name}': {sorted(missing)}",
                    "available_columns": sorted(col.get("name", "") for col in columns),
                }
            columns = [col for col in columns if col.get("name", "").lower() in filter_set]

        # 4. Fetch existing metrics (using table ID, not name)
        table_id = table.get("id")
        if not table_id:
            return {"error": f"Table '{table_name}' found but has no ID"}
        metrics_result = await client.get_table_metrics(
            workspace_id=workspace_id,
            table_ids=[table_id],
        )
        metrics = metrics_result.get("metrics", [])

        # Build per-column covered dimensions from existing metrics
        column_covered_dimensions: Dict[str, set] = {}
        table_covered_dimensions: set = set()
        for m in metrics:
            # Extract column name from parameters list
            params = m.get("parameters", [])
            col = params[0].get("columnName", "") if params else ""
            # Extract metric type from nested metricType structure
            metric_type = (
                m.get("metricType", {})
                .get("predefinedMetric", {})
                .get("metricName", "")
            )
            dim = metric_to_dimension.get(metric_type)
            if dim:
                if metric_type in table_level_names or m.get("isTableMetric", False):
                    table_covered_dimensions.add(dim)
                else:
                    column_covered_dimensions.setdefault(col, set()).add(dim)

        # 5. Determine which dimensions are table-level vs column-level
        table_level_dimensions: List[str] = []
        column_level_dimensions: List[str] = []
        for dim_name, info in dimension_info.items():
            has_non_table_metric = any(
                mt not in table_level_names for mt in info["metric_types"]
            )
            has_table_metric = any(
                mt in table_level_names for mt in info["metric_types"]
            )
            if has_table_metric:
                table_level_dimensions.append(dim_name)
            if has_non_table_metric:
                column_level_dimensions.append(dim_name)

        # 6. Compute per-column coverage
        column_results = []
        total_applicable = 0
        total_covered = 0

        for col in columns:
            col_name = col.get("name", "")
            raw_type = col.get("type", "STRING")
            is_pk = col.get("isPrimaryKey", False)
            is_fk = col.get("isForeignKey", False)

            type_family = _normalize_data_type(raw_type)
            role = _infer_column_role(col_name, type_family, is_pk, is_fk)

            # A dimension is applicable if at least one of its non-table-level
            # metrics passes _metric_applies_to_type for this column's type family
            applicable = []
            applicable_suggested: Dict[str, List[str]] = {}
            for dim_name in column_level_dimensions:
                info = dimension_info[dim_name]
                matching_metrics = [
                    mt for mt in info["metric_types"]
                    if mt not in table_level_names
                    and _metric_applies_to_type(mt, type_family)
                ]
                if matching_metrics:
                    applicable.append(dim_name)
                    # Pick first few as suggested metrics
                    applicable_suggested[dim_name] = matching_metrics[:3]

            covered = column_covered_dimensions.get(col_name, set())
            covered_dims = [d for d in applicable if d in covered]
            uncovered_dims = [d for d in applicable if d not in covered]

            n_applicable = len(applicable)
            n_covered = len(covered_dims)
            total_applicable += n_applicable
            total_covered += n_covered

            col_score = n_covered / n_applicable if n_applicable > 0 else 1.0

            column_results.append({
                "column": col_name,
                "data_type": raw_type,
                "type_family": type_family,
                "inferred_role": role,
                "applicable_dimensions": applicable,
                "covered_dimensions": covered_dims,
                "uncovered_dimensions": uncovered_dims,
                "coverage_score": round(col_score, 2),
                "_suggested": applicable_suggested,
            })

        # 7. Table-level dimension coverage
        table_uncovered = [
            d for d in table_level_dimensions if d not in table_covered_dimensions
        ]
        total_applicable += len(table_level_dimensions)
        total_covered += len(table_covered_dimensions)

        aggregate_score = (
            total_covered / total_applicable if total_applicable > 0 else 1.0
        )

        # 8. Prioritized gap list
        gaps = []
        for dim in table_uncovered:
            info = dimension_info.get(dim, {})
            table_metrics = [
                mt for mt in info.get("metric_types", []) if mt in table_level_names
            ]
            gaps.append({
                "level": "table",
                "dimension": dim,
                "category": info.get("category", ""),
                "suggested_metrics": table_metrics[:3],
            })
        for cr in column_results:
            for dim in cr["uncovered_dimensions"]:
                info = dimension_info.get(dim, {})
                gaps.append({
                    "level": "column",
                    "column": cr["column"],
                    "dimension": dim,
                    "category": info.get("category", ""),
                    "inferred_role": cr["inferred_role"],
                    "suggested_metrics": cr["_suggested"].get(dim, []),
                })

        # Strip internal _suggested from column results
        for cr in column_results:
            cr.pop("_suggested", None)

        full_name_parts = [
            table.get("warehouseName", ""),
            table.get("databaseName", ""),
            table.get("schemaName", ""),
            table.get("name", ""),
        ]
        full_name = ".".join(p for p in full_name_parts if p)

        # Build dimensions summary for agent context
        dimensions_summary = []
        for dim_name, info in dimension_info.items():
            dimensions_summary.append({
                "name": dim_name,
                "category": info["category"],
                "metric_count": len(info["metric_types"]),
            })

        return {
            "table": full_name,
            "aggregate_coverage_score": round(aggregate_score, 2),
            "total_applicable_dimensions": total_applicable,
            "total_covered_dimensions": total_covered,
            "dimensions": dimensions_summary,
            "table_level": {
                "applicable": table_level_dimensions,
                "covered": list(table_covered_dimensions),
                "uncovered": table_uncovered,
            },
            "columns": column_results,
            "gaps": gaps,
        }

    except Exception as e:
        return {
            "error": True,
            "message": f"Error computing dimension coverage: {str(e)}",
        }


@mcp.tool()
async def get_table_dimension_coverage(
    table_name: str = "",
    schema_name: Optional[str] = None,
    table_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Analyze which data quality dimensions are covered by monitors on a table and which have gaps. Automatically joins the workspace's dimension taxonomy, the table's column metadata, and existing metrics to produce: per-column coverage, table-level coverage, an aggregate score, and a prioritized gap list with suggested metric types. This is the recommended single-call tool for answering 'what monitoring is missing on this table?'

    Prefer passing table_id (from search_tables) for reliable lookups. Falls back to name-based search if table_id is not provided.

    Args:
        table_name: Table name to analyze (used as fallback if table_id not provided)
        schema_name: Optional schema name to narrow name-based search
        table_id: Table ID from search_tables — preferred, avoids ambiguous name lookups
    """
    if not table_id and not table_name:
        return {"error": "Either table_id or table_name is required"}
    return await _compute_dimension_coverage(table_name, schema_name, table_id=table_id)


@mcp.tool()
async def get_column_dimension_coverage(
    table_name: str = "",
    column_names: List[str] = [],
    schema_name: Optional[str] = None,
    table_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Analyze dimension coverage for specific columns in a table. Same analysis as get_table_dimension_coverage but filtered to the requested columns. Use this when you already know which columns to investigate — for example, after identifying columns with issues or when a user asks about specific fields. Always includes table-level dimension coverage for context.

    Prefer passing table_id (from search_tables) for reliable lookups. Falls back to name-based search if table_id is not provided.

    Args:
        table_name: Table name to analyze (used as fallback if table_id not provided)
        column_names: List of column names to analyze coverage for
        schema_name: Optional schema name to narrow name-based search
        table_id: Table ID from search_tables — preferred, avoids ambiguous name lookups
    """
    if not table_id and not table_name:
        return {"error": "Either table_id or table_name is required"}
    if not column_names:
        return {"error": "column_names is required"}
    return await _compute_dimension_coverage(table_name, schema_name, column_names, table_id=table_id)


# ---------------------------------------------------------------------------
# Tag Management Tools
# ---------------------------------------------------------------------------

_COLOR_HEX_RE = re.compile(r"^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$")


def _validate_entity_type(entity_type: str) -> tuple:
    """Validate and normalize entity_type. Returns (normalized_value, error_dict_or_None)."""
    upper = entity_type.upper()
    if upper not in _TAGGABLE_ENTITY_TYPE_MAP:
        return None, {
            "error": f"Invalid entity_type: {entity_type}",
            "valid_values": ["SOURCE", "SCHEMA", "TABLE", "METRIC", "COLUMN", "DELTA", "SLA", "CUSTOM_RULE"],
            "hint": "Use short names (e.g. METRIC) or full enum values (e.g. TAGGABLE_ENTITY_TYPE_METRIC)",
        }
    return _TAGGABLE_ENTITY_TYPE_MAP[upper], None


@mcp.tool()
async def list_tags(
    search: Optional[str] = None,
    page_size: int = 50,
    page_cursor: Optional[str] = None,
) -> Dict[str, Any]:
    """List or search workspace tags. Returns tag IDs, names, and colors. Use search to filter by name.

    Args:
        search: Optional search string to filter tags by name
        page_size: Number of tags per page (default 50)
        page_cursor: Pagination cursor from a previous response
    """
    client = get_api_client()

    try:
        result = await client.list_tags(
            search=search,
            page_size=page_size,
            page_cursor=page_cursor,
        )
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        tags = result.get("tags", [])
        formatted = [
            {
                "id": t.get("id"),
                "name": t.get("name"),
                "color_hex": t.get("colorHex"),
            }
            for t in tags
        ]

        response: Dict[str, Any] = {
            "total_returned": len(formatted),
            "tags": formatted,
        }
        pagination = result.get("paginationInfo")
        if pagination and pagination.get("nextCursor"):
            response["next_page_cursor"] = pagination["nextCursor"]
        return response

    except Exception as e:
        return {"error": True, "message": f"Error listing tags: {str(e)}"}


@mcp.tool()
async def create_tag(
    name: str,
    color_hex: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new workspace tag. Tags can be applied to metrics, tables, columns, and other entities.

    Args:
        name: Tag display name (max 60 characters)
        color_hex: Optional hex color with # prefix (e.g. "#FF5733"). Must be 3 or 6 hex digits.
    """
    if len(name) > 60:
        return {"error": f"Tag name too long ({len(name)} chars). Maximum is 60 characters."}

    if color_hex and not _COLOR_HEX_RE.match(color_hex):
        return {"error": f"Invalid color_hex: {color_hex}. Must be # followed by 3 or 6 hex digits (e.g. #FF5733)."}

    client = get_api_client()

    try:
        result = await client.create_tag(name=name, color_hex=color_hex)
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        tag = result.get("tag", result)
        return {
            "success": True,
            "tag": {
                "id": tag.get("id"),
                "name": tag.get("name"),
                "color_hex": tag.get("colorHex"),
            },
        }

    except Exception as e:
        return {"error": True, "message": f"Error creating tag: {str(e)}"}


@mcp.tool()
async def update_tag(
    tag_id: int,
    name: Optional[str] = None,
    color_hex: Optional[str] = None,
) -> Dict[str, Any]:
    """Update an existing workspace tag's name or color.

    Args:
        tag_id: ID of the tag to update
        name: New tag name (optional, max 60 characters)
        color_hex: New hex color with # prefix (optional, e.g. "#FF5733")
    """
    if not name and not color_hex:
        return {"error": "No fields to update. Provide at least one of: name, color_hex"}

    if name and len(name) > 60:
        return {"error": f"Tag name too long ({len(name)} chars). Maximum is 60 characters."}

    if color_hex and not _COLOR_HEX_RE.match(color_hex):
        return {"error": f"Invalid color_hex: {color_hex}. Must be # followed by 3 or 6 hex digits (e.g. #FF5733)."}

    client = get_api_client()

    try:
        result = await client.update_tag(tag_id=tag_id, name=name, color_hex=color_hex)
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        tag = result.get("tag", result)
        return {
            "success": True,
            "tag": {
                "id": tag.get("id"),
                "name": tag.get("name"),
                "color_hex": tag.get("colorHex"),
            },
        }

    except Exception as e:
        return {"error": True, "message": f"Error updating tag: {str(e)}"}


@mcp.tool()
async def delete_tag(
    tag_id: int,
) -> Dict[str, Any]:
    """Delete a workspace tag. This removes the tag from all entities it was applied to.

    Args:
        tag_id: ID of the tag to delete
    """
    client = get_api_client()

    try:
        result = await client.delete_tag(tag_id=tag_id)
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        tag = result.get("tag", result)
        return {
            "success": True,
            "deleted_tag": {
                "id": tag.get("id"),
                "name": tag.get("name"),
            },
        }

    except Exception as e:
        return {"error": True, "message": f"Error deleting tag: {str(e)}"}


@mcp.tool()
async def tag_entity(
    tag_id: int,
    entity_id: int,
    entity_type: str,
) -> Dict[str, Any]:
    """Apply a workspace tag to an entity. The tag must already exist (use create_tag first).

    Args:
        tag_id: ID of the workspace tag to apply
        entity_id: ID of the entity to tag (e.g. metric ID, table ID)
        entity_type: Entity type — SOURCE, SCHEMA, TABLE, METRIC, COLUMN, DELTA, SLA, or CUSTOM_RULE
    """
    normalized_type, err = _validate_entity_type(entity_type)
    if err:
        return err

    client = get_api_client()

    try:
        result = await client.tag_entity(
            tag_id=tag_id,
            entity_id=entity_id,
            entity_type=normalized_type,
        )
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        return {
            "success": True,
            "tag_id": tag_id,
            "entity_id": entity_id,
            "entity_type": normalized_type,
        }

    except Exception as e:
        return {"error": True, "message": f"Error tagging entity: {str(e)}"}


@mcp.tool()
async def untag_entity(
    tag_id: int,
    entity_id: int,
    entity_type: str,
) -> Dict[str, Any]:
    """Remove a workspace tag from an entity.

    Args:
        tag_id: ID of the workspace tag to remove
        entity_id: ID of the entity to untag
        entity_type: Entity type — SOURCE, SCHEMA, TABLE, METRIC, COLUMN, DELTA, SLA, or CUSTOM_RULE
    """
    normalized_type, err = _validate_entity_type(entity_type)
    if err:
        return err

    client = get_api_client()

    try:
        result = await client.untag_entity(
            tag_id=tag_id,
            entity_id=entity_id,
            entity_type=normalized_type,
        )
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        return {
            "success": True,
            "tag_id": tag_id,
            "entity_id": entity_id,
            "entity_type": normalized_type,
        }

    except Exception as e:
        return {"error": True, "message": f"Error untagging entity: {str(e)}"}


@mcp.tool()
async def list_entity_tags(
    entity_id: int,
    entity_type: str,
) -> Dict[str, Any]:
    """List all tags applied to a specific entity.

    Args:
        entity_id: ID of the entity
        entity_type: Entity type — SOURCE, SCHEMA, TABLE, METRIC, COLUMN, DELTA, SLA, or CUSTOM_RULE
    """
    normalized_type, err = _validate_entity_type(entity_type)
    if err:
        return err

    client = get_api_client()

    try:
        result = await client.get_entity_tags(
            entity_type=normalized_type,
            entity_id=entity_id,
        )
        if isinstance(result, dict) and result.get("error"):
            return {
                "error": f"API error (status {result.get('status_code', 'unknown')}): {result.get('message', 'Unknown error')}",
            }

        # v1 endpoint returns {"entityId": ..., "entityType": ..., "tags": ["name1", ...]}
        tags = result.get("tags", [])
        return {
            "entity_id": entity_id,
            "entity_type": normalized_type,
            "tags": tags,
            "total": len(tags),
        }

    except Exception as e:
        return {"error": True, "message": f"Error listing entity tags: {str(e)}"}


# Run the server if executed directly
if __name__ == "__main__":
    mcp.run()