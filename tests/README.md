# MCP Protocol Testing Documentation

This directory contains comprehensive tests for verifying the Bigeye MCP Server's stdio communication protocol compliance.

## Overview

The test suite validates that the Docker container correctly implements the Model Context Protocol (MCP) by:
1. Testing the initialization handshake
2. Verifying tool, resource, and prompt discovery
3. Executing tool calls
4. Reading resources
5. Handling errors appropriately

## Test Components

### 1. Test Fixtures (`fixtures/mcp_messages.json`)

Contains JSON-RPC messages used for testing:
- **Initialize**: MCP handshake with capabilities negotiation
- **List Tools**: Discover available tools
- **List Resources**: Discover available resources
- **List Prompts**: Discover available prompts
- **Call Tool**: Execute the `check_health` tool
- **Read Resource**: Read the `bigeye://config` resource
- **Error Handling**: Test invalid method handling

### 2. MCP Test Client (`mcp_test_client.py`)

A Python client that:
- Starts the Docker container with proper stdio setup
- Sends JSON-RPC messages via stdin
- Reads responses from stdout
- Validates response structure and content
- Provides debug logging capabilities

### 3. Test Scripts

- **`test-mcp-protocol.sh`**: Standalone MCP protocol test runner
- **`test.sh`**: Main test script with optional MCP tests via `--mcp` or `--full` flag

## Running the Tests

### Quick Test
```bash
# Run basic container tests
./scripts/test.sh

# Run basic tests + MCP protocol tests
./scripts/test.sh --mcp
```

### Standalone MCP Tests
```bash
# Run MCP protocol tests
./scripts/test-mcp-protocol.sh

# Run with debug output
./scripts/test-mcp-protocol.sh latest --debug
```

### Python Test Client
```bash
# Run the test client directly
python tests/mcp_test_client.py --image bigeye-mcp-server:latest

# With debug output
python tests/mcp_test_client.py --debug

# With custom env file
python tests/mcp_test_client.py --env-file .env.test
```

## Test Sequence

1. **Container Startup**
   - Launches Docker container with test environment
   - Establishes stdio communication channels
   - Waits for server initialization

2. **Initialize Handshake**
   ```json
   → {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {...}}
   ← {"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "0.1.0", ...}}
   ```

3. **Discovery Tests**
   - Lists all available tools (12+ expected)
   - Lists all resources (3 expected)
   - Lists all prompts (5 expected)

4. **Tool Execution**
   ```json
   → {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "check_health", ...}}
   ← {"jsonrpc": "2.0", "id": 5, "result": {"content": [{"type": "text", "text": "..."}]}}
   ```

5. **Resource Reading**
   ```json
   → {"jsonrpc": "2.0", "id": 6, "method": "resources/read", "params": {"uri": "bigeye://config"}}
   ← {"jsonrpc": "2.0", "id": 6, "result": {"contents": [{"text": "{...}"}]}}
   ```

6. **Error Handling**
   - Tests invalid method calls
   - Verifies proper error responses

## Expected Results

All tests should pass with output similar to:
```
=== Test 1: Initialize Handshake ===
✓ Initialize: PASS
  Server: Bigeye API
  Version: 0.1.0

=== Test 2: List Tools ===
✓ List Tools: PASS
  Found 12 tools

=== Test 3: List Resources ===
✓ List Resources: PASS
  Found 3 resources

=== Test 4: Call check_health Tool ===
✓ Call check_health: PASS
  Response: API Health Status: healthy...

=== Test 5: Read Config Resource ===
✓ Read Config: PASS

=== Test 6: Error Handling ===
✓ Error Handling: PASS
  Error: Method not found

=== Test Summary ===
✓ initialize: PASS
✓ list_tools: PASS
✓ list_resources: PASS
✓ call_check_health: PASS
✓ read_config_resource: PASS
✓ error_handling: PASS

Total: 6/6 tests passed
```

## Debugging Failed Tests

### Enable Debug Mode
```bash
./scripts/test-mcp-protocol.sh latest --debug
```

This will show:
- All JSON-RPC messages sent/received
- Container stderr output
- Detailed error messages

### Common Issues

1. **Container Won't Start**
   - Check Docker is running: `docker info`
   - Verify image exists: `docker images | grep bigeye-mcp`
   - Check port conflicts

2. **No Response from Server**
   - Server may be buffering output - ensure Python unbuffered mode
   - Check for Python errors in stderr with debug mode
   - Verify MCP dependencies are installed

3. **JSON Parse Errors**
   - Server may be outputting non-JSON debug info
   - Check stderr for actual errors
   - Ensure PYTHONUNBUFFERED=1 is set

4. **Tool Execution Fails**
   - Verify API credentials are set (even test values)
   - Check tool implementation for exceptions
   - Review server logs in debug mode

## Extending the Tests

To add new test cases:

1. Add message fixtures to `fixtures/mcp_messages.json`
2. Add test logic to `mcp_test_client.py`
3. Update expected results documentation

Example fixture:
```json
"new_test": {
  "request": {
    "jsonrpc": "2.0",
    "id": 10,
    "method": "tools/call",
    "params": {
      "name": "get_issues",
      "arguments": {"page_size": 10}
    }
  },
  "expected_response_fields": ["issues", "page_info"]
}
```

## Integration with CI/CD

The test suite can be integrated into CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Build Docker image
  run: ./scripts/build.sh

- name: Run MCP protocol tests
  run: ./scripts/test-mcp-protocol.sh
```

## Performance Considerations

- Tests typically complete in 10-15 seconds
- Debug mode adds ~5 seconds overhead
- Container startup takes ~2 seconds
- Each JSON-RPC round trip: ~100-500ms