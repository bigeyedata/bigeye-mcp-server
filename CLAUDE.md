# Claude Development Notes

## Configuration

- Environment variables: `BIGEYE_API_KEY`, `BIGEYE_BASE_URL`, `BIGEYE_WORKSPACE_ID`
- Docker image must be tagged with both names: `bigeye-mcp-server:latest` and `bigeye-mcp-ephemeral:latest`

## Workflow Guidelines

### Issue ID vs Display Name
- `id` = internal database ID (e.g. 12345) — used for API operations
- `name` = display reference (e.g. "10921", "TU-7573") — what users see in the UI
- When a user references an issue by number, use `search_issues(name_query="10921")` to resolve the display name to an internal ID

## Testing

1. Rebuild the Docker image with both tags:
   ```bash
   docker build -t bigeye-mcp-server:latest -t bigeye-mcp-ephemeral:latest .
   ```
2. Test with Claude Desktop after rebuilding
3. Commit changes with descriptive messages

## API Quirks

- The `POST /api/v1/search` endpoint is broken — it ignores `searchString` and filter parameters, returning all issues in a fixed order. Only `limit` works. Catalog search tools are temporarily removed pending improvements (see ONE-12139).
- Workspace IDs must be integers, not strings
- Some endpoints use camelCase, others use snake_case
- Search endpoints require exact matches with underscores (e.g. `sales_dashboard` not `sales dashboard`)
- The Bigeye API returns `issue` (singular) as the response key, not `issues` (plural)
- Issue fields like `tableName`/`schemaName` are nested in `metricMetadata`, not at top level

## Roadmap

See [TODO.md](TODO.md) for planned tools, resources, and improvements.
