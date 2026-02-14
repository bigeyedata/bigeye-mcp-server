# Claude Development Notes

## Configuration

- Environment variables: `BIGEYE_API_KEY`, `BIGEYE_API_URL`, `BIGEYE_WORKSPACE_ID`
- Docker image must be tagged with both names: `bigeye-mcp-server:latest` and `bigeye-mcp-ephemeral:latest`

## Workflow Guidelines

### Tables/Columns
1. **ALWAYS search first** using `search_tables()` or `search_columns()`
2. Present results as a numbered list
3. Ask user to confirm which object they mean
4. Then proceed with analysis/health checks

### Issues/Incidents

**Issue ID vs Name:**
- `id` = internal database ID (e.g. 12345) — users typically don't know this
- `name` = display reference (e.g. "10921") — what users see and reference

**Workflow:**
1. **ALWAYS use `search_issues()` first** when users reference an issue by number
2. Present results with name, status, description, affected tables
3. Use the returned `id` for subsequent operations

```
User: "Show me incident 10921"
WRONG:   list_related_issues(starting_issue_id=10921)
CORRECT: search_issues(name_query="10921") → use returned 'id'
```

### Issue Listing
1. `list_issues(compact=True)` → lightweight list
2. User identifies issue of interest
3. `get_issue(issue_id=...)` → full details

## Testing

1. Rebuild the Docker image with both tags:
   ```bash
   docker build -t bigeye-mcp-server:latest -t bigeye-mcp-ephemeral:latest .
   ```
2. Test with Claude Desktop after rebuilding
3. Commit changes with descriptive messages

## API Quirks

- The `/api/v1/search` endpoint doesn't work with workspace ID. Use `/api/v1/tables`, `/api/v1/columns`, `/api/v1/schemas` instead.
- Workspace IDs must be integers, not strings
- Some endpoints use camelCase, others use snake_case
- Search endpoints require exact matches with underscores (e.g. `sales_dashboard` not `sales dashboard`)
- The Bigeye API returns `issue` (singular) as the response key, not `issues` (plural)
- Issue fields like `tableName`/`schemaName` are nested in `metricMetadata`, not at top level

## Roadmap

See [TODO.md](TODO.md) for planned tools, resources, and improvements.
