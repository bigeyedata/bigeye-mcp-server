# Claude Development Notes

## Configuration

- Environment variables: `BIGEYE_API_KEY`, `BIGEYE_BASE_URL`, `BIGEYE_WORKSPACE_ID`
- Docker image must be tagged with both names: `bigeye-mcp-server:latest` and `bigeye-mcp-ephemeral:latest`

## Transports

The same image runs in two modes; the Dockerfile defaults to HTTP:

- **stdio** (`python server.py`) — original Claude Desktop path, env vars are the credentials. Compose service: `bigeye-mcp`. Used by `mcp-wrapper.sh` / `bigeye-mcp.sh`.
- **HTTP / BYOK** (`python server.py --http`, `:9101`) — for cloud agents like the Bigeye DRE Agent. No env-var credentials; each request supplies them as headers:
  - `Authorization: Bearer <bigeye-api-key>`
  - `X-Bigeye-Base-Url: https://app.bigeye.com`
  - `X-Bigeye-Workspace-Id: <int>`

  Compose services: `bigeye-mcp-http` (the server) and `ngrok` (under `--profile tunnel`). Public URL: `https://bigeye-mcp.ngrok.app/mcp`. ngrok IP allowlist policy: `ipp_3D5i0vsFRuy9jDdyEj8KfuBsWPE` (in `infra/ngrok-policy.yml`).

Per-request credentials flow through `auth_context.py`'s contextvar; `BigeyeAPIClient` and `config.get(...)` both consult it before falling back to env vars, so existing tool code didn't need changes.

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
