# Claude Development Notes

This file contains important information and to-do items for Claude when working on this MCP server.

## Configuration

- The Bigeye MCP server uses environment variables for configuration
- Workspace ID is automatically retrieved from `BIGEYE_WORKSPACE_ID` environment variable
- API credentials are passed via `BIGEYE_API_KEY` and `BIGEYE_API_URL`
- Docker image must be tagged with both names: `bigeye-mcp-server:latest` and `bigeye-mcp-ephemeral:latest`

## Workflow Guidelines

### When Users Ask About Tables/Columns
1. **ALWAYS search first** using `search_tables()` or `search_columns()` tools
2. Present search results as a numbered list
3. Ask user to confirm which specific object they mean
4. Only then proceed with analysis/health checks

This workflow is enforced in tool descriptions with "ALWAYS USE THIS TOOL FIRST" instructions.

## Known Issues & To-Do Items

### High Priority

#### 1. ~~Fix Get Issues Response Size~~ ✅ COMPLETED
**Issue**: The `get_issues` tool responses are too large. The API is returning not only the issues but also all associated run history, which can be extremely long and overwhelm the context.

**FIXED**: Added response optimization in `fetch_issues()` that:
- Strips out historical metric runs, keeping only essential metadata
- Limits events to just the most recent one
- Removes large fields like `metricRunHistory`, `detailedHistory`, `allEvents`
- Added `include_full_history` parameter (defaults to False) for when full history is needed
- Set default page_size to 20 to limit results

### Future Improvements

- Add more granular filtering options for search results
- Implement caching for frequently accessed data
- Add support for bulk operations on issues
- Consider adding a tool to get issue details separately from the list
- **Search improvements needed**:
  - Add automatic space-to-underscore conversion in table/column searches
  - Implement fuzzy matching or wildcards for more flexible searches
  - Add search result caching to avoid repeated API calls
  - Consider parallel searching across Bigeye and Atlan
- **Cross-system integration**:
  - Add mapping between Bigeye and Atlan naming conventions
  - Help correlate the same assets across both systems
- **Error handling**:
  - Fix JSON parsing exceptions in Bigeye responses
  - Add retry logic for recoverable errors
  - Better error messages when searches return no results

## Testing

When making changes:
1. Always rebuild the Docker image with both tags:
   ```bash
   docker build -t bigeye-mcp-server:latest -t bigeye-mcp-ephemeral:latest .
   ```
2. Test with Claude Desktop after rebuilding
3. Commit changes with descriptive messages

## API Quirks

- ~~The `/api/v1/search` endpoint requires `workspaceId` as a query parameter, not in the request body~~ **UPDATE**: The `/api/v1/search` endpoint doesn't work with workspace ID at all. We now use separate `/api/v1/tables`, `/api/v1/columns`, and `/api/v1/schemas` endpoints instead.
- Workspace IDs must be integers, not strings
- Some endpoints use camelCase while others use snake_case - be careful with parameter names
- Search endpoints require exact matches with underscores (e.g., "sales_dashboard" not "sales dashboard")
- The `/api/v1/tables`, `/api/v1/columns`, `/api/v1/schemas` endpoints properly accept `workspaceId` as a query parameter