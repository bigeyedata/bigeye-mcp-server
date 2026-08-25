# Claude Development Notes

## Configuration

- Environment variables: `BIGEYE_API_KEY`, `BIGEYE_BASE_URL`, `BIGEYE_WORKSPACE_ID`
- Optional env vars: `BIGEYE_DEBUG` (verbose logging), `BIGEYE_TELEMETRY` (anonymous usage telemetry, opt-out — set `false` to disable)
- Telemetry ships metadata-only tool-call events to Bigeye's Datadog via a committed public Datadog client token (`telemetry.py`). No build-arg or per-customer setup needed.
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

- The `POST /api/v1/search` endpoint is broken — it ignores `searchString` and filter parameters, returning all issues in a fixed order. Only `limit` works. Do not use it. Catalog search (`search_schemas`/`search_tables`/`search_columns`) is implemented on `POST /api/v2/search`, which honors the `search` query string, `types` filter, and `limit`. The v2 body uses protobuf-JSON: `{"search": "...", "types": [{"dataNodeType": "DATA_NODE_TYPE_TABLE"}], "limit": N}` — workspace comes from the `x-bigeye-workspace-id` header, not the body.
- Workspace IDs must be integers, not strings
- Some endpoints use camelCase, others use snake_case
- Search endpoints require exact matches with underscores (e.g. `sales_dashboard` not `sales dashboard`)
- The Bigeye API returns `issue` (singular) as the response key, not `issues` (plural)
- Issue fields like `tableName`/`schemaName` are nested in `metricMetadata`, not at top level
- Glossary term listing is `POST /api/v1/glossary-terms/fetch`, not a `GET` with query params, matching `GlossaryTermResource.fetch`. Link/unlink/fetch-for-entities request bodies wrap the ref list under the key `refs`, not `entityRefs`. `PUT /api/v1/glossary-terms/{id}` is a full field replacement, not a patch — omitted fields are left alone, but a provided `synonyms` list replaces the whole list, so adding one synonym requires fetching the term first and sending back the full merged list.

## Roadmap

See [TODO.md](TODO.md) for planned tools, resources, and improvements.
