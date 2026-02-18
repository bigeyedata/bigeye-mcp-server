# Tag Management Tools — Design

## Goal

Add MCP tools for creating, listing, and applying workspace tags to any Bigeye entity. The immediate use case is agents tagging metrics they create (e.g. "Created by DRE") but the design supports all taggable entity types.

## Decisions

- **V2 workspace tags only** — modern API at `/api/v2/tags`, workspace-scoped, supports colors
- **All taggable entity types** — metrics, tables, columns, schemas, sources, deltas, SLAs, custom rules
- **Full CRUD + tag/untag** — 7 tools total
- **No auto-create** — agent must explicitly create tags before applying them

## API Client Layer (`bigeye_api.py`)

Seven new methods on `BigeyeAPI`:

| Method | HTTP | Endpoint | Key Params |
|--------|------|----------|------------|
| `list_tags()` | POST | `/api/v2/tags/fetch` | `workspace_id`, `search`, `page_size`, `page_cursor` |
| `create_tag()` | POST | `/api/v2/tags` | `name`, `color_hex` (optional) |
| `update_tag()` | PUT | `/api/v2/tags/{id}` | `tag_id`, `name`, `color_hex` |
| `delete_tag()` | DELETE | `/api/v2/tags/{id}` | `tag_id` |
| `tag_entity()` | POST | `/api/v2/tags/tag` | `tag_id`, `entity_id`, `entity_type` |
| `untag_entity()` | POST | `/api/v2/tags/untag` | `tag_id`, `entity_id`, `entity_type` |
| `get_entity_tags()` | GET | `/api/v1/tags/{entity_type}/{entity_id}` | `entity_type`, `entity_id` |

`get_entity_tags` uses the v1 endpoint because v2 has no "all tags on entity X" query — only "all entities with tag Y". This is a read-only call returning flat tag name strings.

### Proto reference

```protobuf
message Tag {
  optional int32 id = 1;
  optional string name = 2;
  optional string color_hex = 3;   // includes # prefix
  optional int32 workspace_id = 4;
  optional int64 created_at = 5;
  optional int64 updated_at = 6;
}

message TagRequest {
  optional TagItem tag = 1;
  optional bool create_tag_if_does_not_exist = 2 [default = false];
}

message TagItem {
  optional int32 entity_id = 1;
  optional TaggableEntityType entity_type = 2;
  optional string entity_name = 3;
  optional string tag = 4;              // legacy flat tag
  optional Tag workspace_tag = 5;       // structured workspace tag
}

enum TaggableEntityType {
  TAGGABLE_ENTITY_TYPE_UNSPECIFIED = 0;
  TAGGABLE_ENTITY_TYPE_SOURCE = 1;
  TAGGABLE_ENTITY_TYPE_SCHEMA = 2;
  TAGGABLE_ENTITY_TYPE_DATASET = 3;    // table
  TAGGABLE_ENTITY_TYPE_METRIC = 4;
  TAGGABLE_ENTITY_TYPE_DELTA = 5;
  TAGGABLE_ENTITY_TYPE_COLUMN = 6;
  TAGGABLE_ENTITY_TYPE_SLA = 7;
  TAGGABLE_ENTITY_TYPE_CUSTOM_RULE = 15;
}
```

## MCP Tool Layer (`server.py`)

### Entity type mapping

```python
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

### Tools

| Tool | Params | Returns |
|------|--------|---------|
| `list_tags` | `search` (opt), `page_size` (opt, default 50), `page_cursor` (opt) | `{tags: [{id, name, color_hex}], pagination}` |
| `create_tag` | `name` (req), `color_hex` (opt) | `{tag: {id, name, color_hex}}` |
| `update_tag` | `tag_id` (req), `name` (opt), `color_hex` (opt) | `{tag: {id, name, color_hex}}` |
| `delete_tag` | `tag_id` (req) | `{success: true, deleted_tag: {...}}` |
| `tag_entity` | `tag_id` (req), `entity_id` (req), `entity_type` (req) | `{success: true, tag_id, entity_id, entity_type}` |
| `untag_entity` | `tag_id` (req), `entity_id` (req), `entity_type` (req) | `{success: true}` |
| `list_entity_tags` | `entity_id` (req), `entity_type` (req) | `{entity_id, entity_type, tags: ["tag1", ...]}` |

### Validation

- `entity_type`: validated against `_TAGGABLE_ENTITY_TYPE_MAP`, case-insensitive. Error returns valid values.
- `color_hex`: regex `^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$`. Error returns format hint.
- `name`: max 60 characters.
- API error responses: checked via `result.get("error")` pattern (same as `create_metric`).

## Documentation

New **Tags** section in README.md after "Data Dimensions", before "System":

```
### Tags

- **`list_tags`** — List or search workspace tags
- **`create_tag`** — Create a new tag with optional color
- **`update_tag`** — Update a tag's name or color
- **`delete_tag`** — Delete a tag
- **`tag_entity`** — Apply a tag to any entity (metric, table, column, etc.)
- **`untag_entity`** — Remove a tag from an entity
- **`list_entity_tags`** — List all tags on a specific entity
```

## Verification

1. Syntax check: `python3 -c "import ast; ast.parse(open('bigeye_api.py').read()); ast.parse(open('server.py').read())"`
2. Docker rebuild: `./bigeye-mcp.sh rebuild`
3. Manual test: create a tag, apply it to a metric, list tags on the metric, untag, delete
