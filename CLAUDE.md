# Claude Development Notes

This file contains important information and to-do items for Claude when working on this MCP server.

## Configuration

- The Bigeye MCP server uses environment variables for configuration
- Workspace ID is automatically retrieved from `BIGEYE_WORKSPACE_ID` environment variable
- API credentials are passed via `BIGEYE_API_KEY` and `BIGEYE_API_URL`

## Known Issues & To-Do Items

### High Priority

#### 1. Fix Get Issues Response Size
**Issue**: The `get_issues` tool responses are too large. The API is returning not only the issues but also all associated run history, which can be extremely long and overwhelm the context.

**To Fix**:
- Investigate the `/api/v1/issues/fetch` endpoint parameters to see if run history can be excluded
- Consider adding pagination or limiting the response
- May need to filter out run history data in the response processing
- Alternative: Create a separate tool for getting issue details with run history when needed

### Future Improvements

- Add more granular filtering options for search results
- Implement caching for frequently accessed data
- Add support for bulk operations on issues
- Consider adding a tool to get issue details separately from the list

## Testing

When making changes:
1. Always rebuild the Docker image with both tags:
   ```bash
   docker build -t bigeye-mcp-server:latest -t bigeye-mcp-ephemeral:latest .
   ```
2. Test with Claude Desktop after rebuilding
3. Commit changes with descriptive messages

## API Quirks

- The `/api/v1/search` endpoint requires `workspaceId` as a query parameter, not in the request body
- Workspace IDs must be integers, not strings
- Some endpoints use camelCase while others use snake_case - be careful with parameter names