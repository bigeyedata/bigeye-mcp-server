# Tag Management Tools Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add 7 MCP tools for workspace tag CRUD and entity tagging, enabling agents to tag metrics, tables, and other Bigeye objects.

**Architecture:** Thin wrapper approach — 7 new methods in `bigeye_api.py` (one per API endpoint), 7 new MCP tools in `server.py` with entity-type validation and error checking. Uses v2 workspace tags API for mutations, v1 for "list tags on entity" read.

**Tech Stack:** Python 3.12, MCP SDK (`@mcp.tool()`), httpx (async HTTP via existing `make_request`)

---

### Task 1: Add entity type mapping constant to `server.py`

**Files:**
- Modify: `server.py:118` (after `_LOOKBACK_INTERVAL_TYPE_MAP`)

**Step 1: Add the constant**

Insert after line 118 (after the closing `}` of `_LOOKBACK_INTERVAL_TYPE_MAP`):

```python
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
```

**Step 2: Add `import re` at the top of `server.py`**

Check if `re` is already imported. If not, add `import re` near the other imports at the top of `server.py`. This is needed for color_hex validation in later tasks.

**Step 3: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('server.py').read()); print('OK')"`
Expected: `OK`

**Step 4: Commit**

```bash
git add server.py
git commit -m "feat: add taggable entity type mapping constant for tag tools"
```

---

### Task 2: Add 7 API client methods to `bigeye_api.py`

**Files:**
- Modify: `bigeye_api.py` (append before the final method `search_lineage_v2` at line 1575, or at end of class)

All methods go inside `class BigeyeAPIClient`. Add them after the `create_metric` method (line 1573) and before `search_lineage_v2` (line 1575).

**Step 1: Add `list_tags` method**

```python
    async def list_tags(
        self,
        search: Optional[str] = None,
        page_size: int = 50,
        page_cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List workspace tags via POST /api/v2/tags/fetch.

        Args:
            search: Optional search string to filter tags by name
            page_size: Number of tags per page (default 50)
            page_cursor: Cursor for pagination
        """
        payload: Dict[str, Any] = {
            "workspaceId": self.workspace_id,
            "pageSize": page_size,
        }
        if search:
            payload["search"] = search
        if page_cursor:
            payload["pageCursor"] = page_cursor

        return await self.make_request(
            "/api/v2/tags/fetch",
            method="POST",
            json_data=payload,
        )
```

**Step 2: Add `create_tag` method**

```python
    async def create_tag(
        self,
        name: str,
        color_hex: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a workspace tag via POST /api/v2/tags.

        Args:
            name: Tag display name (max 60 chars)
            color_hex: Optional hex color with # prefix (e.g. "#FF5733")
        """
        tag: Dict[str, Any] = {
            "name": name,
            "workspaceId": self.workspace_id,
        }
        if color_hex:
            tag["colorHex"] = color_hex

        return await self.make_request(
            "/api/v2/tags",
            method="POST",
            json_data={"tag": tag},
        )
```

**Step 3: Add `update_tag` method**

```python
    async def update_tag(
        self,
        tag_id: int,
        name: Optional[str] = None,
        color_hex: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Update a workspace tag via PUT /api/v2/tags/{id}.

        Args:
            tag_id: ID of the tag to update
            name: New tag name (optional)
            color_hex: New hex color with # prefix (optional)
        """
        tag: Dict[str, Any] = {"id": tag_id}
        if name:
            tag["name"] = name
        if color_hex:
            tag["colorHex"] = color_hex

        return await self.make_request(
            f"/api/v2/tags/{tag_id}",
            method="PUT",
            json_data={"tag": tag},
        )
```

**Step 4: Add `delete_tag` method**

```python
    async def delete_tag(self, tag_id: int) -> Dict[str, Any]:
        """Delete a workspace tag via DELETE /api/v2/tags/{id}.

        Args:
            tag_id: ID of the tag to delete
        """
        return await self.make_request(
            f"/api/v2/tags/{tag_id}",
            method="DELETE",
        )
```

**Step 5: Add `tag_entity` method**

```python
    async def tag_entity(
        self,
        tag_id: int,
        entity_id: int,
        entity_type: str,
    ) -> Dict[str, Any]:
        """Apply a workspace tag to an entity via POST /api/v2/tags/tag.

        Args:
            tag_id: ID of the workspace tag
            entity_id: ID of the entity to tag
            entity_type: Enum string e.g. TAGGABLE_ENTITY_TYPE_METRIC
        """
        return await self.make_request(
            "/api/v2/tags/tag",
            method="POST",
            json_data={
                "tag": {
                    "entityId": entity_id,
                    "entityType": entity_type,
                    "workspaceTag": {"id": tag_id},
                },
                "createTagIfDoesNotExist": False,
            },
        )
```

**Step 6: Add `untag_entity` method**

```python
    async def untag_entity(
        self,
        tag_id: int,
        entity_id: int,
        entity_type: str,
    ) -> Dict[str, Any]:
        """Remove a workspace tag from an entity via POST /api/v2/tags/untag.

        Args:
            tag_id: ID of the workspace tag
            entity_id: ID of the entity to untag
            entity_type: Enum string e.g. TAGGABLE_ENTITY_TYPE_METRIC
        """
        return await self.make_request(
            "/api/v2/tags/untag",
            method="POST",
            json_data={
                "tag": {
                    "entityId": entity_id,
                    "entityType": entity_type,
                    "workspaceTag": {"id": tag_id},
                },
            },
        )
```

**Step 7: Add `get_entity_tags` method**

```python
    async def get_entity_tags(
        self,
        entity_type: str,
        entity_id: int,
    ) -> Dict[str, Any]:
        """Get all tags on an entity via GET /api/v1/tags/{entity_type}/{entity_id}.

        Uses the v1 endpoint because v2 has no 'all tags on entity X' query.

        Args:
            entity_type: Enum string e.g. TAGGABLE_ENTITY_TYPE_METRIC
            entity_id: ID of the entity
        """
        return await self.make_request(
            f"/api/v1/tags/{entity_type}/{entity_id}",
        )
```

**Step 8: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('bigeye_api.py').read()); print('OK')"`
Expected: `OK`

**Step 9: Commit**

```bash
git add bigeye_api.py
git commit -m "feat: add 7 tag API client methods (list, create, update, delete, tag, untag, get)"
```

---

### Task 3: Add `list_tags` and `create_tag` MCP tools to `server.py`

**Files:**
- Modify: `server.py` (insert new tools before `# Run the server` block at line 3514, after the last `@mcp.tool()` function `get_column_dimension_coverage`)

**Step 1: Add `list_tags` tool**

Insert after line 3511 (end of `get_column_dimension_coverage`), before line 3514 (`# Run the server`):

```python

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
```

**Step 2: Add `create_tag` tool**

```python
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
```

**Step 3: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('server.py').read()); print('OK')"`
Expected: `OK`

**Step 4: Commit**

```bash
git add server.py
git commit -m "feat: add list_tags and create_tag MCP tools"
```

---

### Task 4: Add `update_tag` and `delete_tag` MCP tools to `server.py`

**Files:**
- Modify: `server.py` (append after `create_tag` tool, before `# Run the server`)

**Step 1: Add `update_tag` tool**

```python
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
```

**Step 2: Add `delete_tag` tool**

```python
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
```

**Step 3: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('server.py').read()); print('OK')"`
Expected: `OK`

**Step 4: Commit**

```bash
git add server.py
git commit -m "feat: add update_tag and delete_tag MCP tools"
```

---

### Task 5: Add `tag_entity`, `untag_entity`, and `list_entity_tags` MCP tools to `server.py`

**Files:**
- Modify: `server.py` (append after `delete_tag` tool, before `# Run the server`)

**Step 1: Add `tag_entity` tool**

```python
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
```

**Step 2: Add `untag_entity` tool**

```python
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
```

**Step 3: Add `list_entity_tags` tool**

```python
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
```

**Step 4: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('server.py').read()); print('OK')"`
Expected: `OK`

**Step 5: Commit**

```bash
git add server.py
git commit -m "feat: add tag_entity, untag_entity, and list_entity_tags MCP tools"
```

---

### Task 6: Update README.md documentation

**Files:**
- Modify: `README.md:204` (insert new Tags section between "Data Dimensions" and "System")

**Step 1: Add Tags section**

Insert after line 203 (end of Data Dimensions section), before line 205 (`### System`):

```markdown
### Tags

- **`list_tags`** — List or search workspace tags
- **`create_tag`** — Create a new tag with optional color
- **`update_tag`** — Update a tag's name or color
- **`delete_tag`** — Delete a tag
- **`tag_entity`** — Apply a tag to any entity (metric, table, column, etc.)
- **`untag_entity`** — Remove a tag from an entity
- **`list_entity_tags`** — List all tags on a specific entity

```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add Tags section to README tool listing"
```

---

### Task 7: Rebuild Docker and verify

**Step 1: Rebuild**

Run: `./bigeye-mcp.sh rebuild`
Expected: Image builds successfully, container starts.

**Step 2: Verify tools are registered**

Check the server logs to confirm the 7 new tools appear in the tool listing. Run: `./bigeye-mcp.sh logs` and look for the tag tool names.

**Step 3: End-to-end test (manual)**

Using Claude Desktop or the MCP inspector, run this sequence:
1. `list_tags` — should return existing tags (may be empty)
2. `create_tag(name="Test Agent Tag", color_hex="#FF5733")` — should return `{success: true, tag: {id: N, ...}}`
3. `list_tags(search="Test Agent")` — should find the new tag
4. `tag_entity(tag_id=N, entity_id=<some_metric_id>, entity_type="METRIC")` — should succeed
5. `list_entity_tags(entity_id=<some_metric_id>, entity_type="METRIC")` — should show the tag
6. `untag_entity(tag_id=N, entity_id=<some_metric_id>, entity_type="METRIC")` — should succeed
7. `delete_tag(tag_id=N)` — should succeed

If any step fails, debug using `./bigeye-mcp.sh logs` and the `[BIGEYE API VERBOSE]` output.
