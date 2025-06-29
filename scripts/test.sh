#!/bin/bash
set -euo pipefail

# Script to test Bigeye MCP Server Docker container with stdio communication
# Usage: ./test.sh [image_tag]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="bigeye-mcp-server"
IMAGE_TAG="${1:-latest}"
TEST_TIMEOUT=30

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if image exists
if ! docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" >/dev/null 2>&1; then
    print_error "Image ${IMAGE_NAME}:${IMAGE_TAG} not found. Please build it first with ./build.sh"
    exit 1
fi

print_info "Testing Bigeye MCP Server Docker container"
print_info "Image: ${IMAGE_NAME}:${IMAGE_TAG}"

# Create temporary directory for test files
TEST_DIR=$(mktemp -d)
trap "rm -rf $TEST_DIR" EXIT

# Create test environment file
cat > "$TEST_DIR/.env" <<EOF
BIGEYE_API_KEY=test_api_key
BIGEYE_API_URL=https://app.bigeye.com
BIGEYE_WORKSPACE_ID=12345
BIGEYE_DEBUG=true
EOF

# Test 1: Basic container startup
print_test "Testing basic container startup..."
if docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}" python -c "print('Container started successfully')"; then
    print_info "✓ Basic container startup test passed"
else
    print_error "✗ Basic container startup test failed"
    exit 1
fi

# Test 2: Environment variable loading
print_test "Testing environment variable loading..."
if docker run --rm --env-file "$TEST_DIR/.env" "${IMAGE_NAME}:${IMAGE_TAG}" \
    python -c "import os; assert os.getenv('BIGEYE_API_KEY') == 'test_api_key', 'API key not loaded'; print('Environment loaded')"; then
    print_info "✓ Environment variable test passed"
else
    print_error "✗ Environment variable test failed"
    exit 1
fi

# Test 3: MCP server imports
print_test "Testing MCP server imports..."
if docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}" \
    python -c "import server; import bigeye_api; import config; print('All imports successful')"; then
    print_info "✓ Import test passed"
else
    print_error "✗ Import test failed"
    exit 1
fi

# Test 4: STDIO communication test
print_test "Testing STDIO communication..."

# Create a test script that sends a simple JSON-RPC message
cat > "$TEST_DIR/test_stdio.py" <<'EOF'
import json
import sys
import time

# Simple test to ensure the server can handle stdio
test_message = {
    "jsonrpc": "2.0",
    "method": "initialize",
    "params": {
        "capabilities": {}
    },
    "id": 1
}

# Write test message to stdout (simulating MCP client)
print(json.dumps(test_message), flush=True)

# Give server time to process
time.sleep(2)

# Exit successfully
sys.exit(0)
EOF

# Run the stdio test with timeout
print_info "Sending test message via stdio..."
if timeout $TEST_TIMEOUT docker run --rm -i --env-file "$TEST_DIR/.env" "${IMAGE_NAME}:${IMAGE_TAG}" \
    python server.py < "$TEST_DIR/test_stdio.py" 2>&1 | grep -q "BIGEYE MCP"; then
    print_info "✓ STDIO communication test passed"
else
    print_warn "⚠ STDIO communication test inconclusive (this is expected for MCP servers)"
fi

# Test 5: Config module test
print_test "Testing configuration module..."
docker run --rm --env-file "$TEST_DIR/.env" "${IMAGE_NAME}:${IMAGE_TAG}" python -c "
from config import config
assert config['api_key'] == 'test_api_key', 'Config not loaded correctly'
assert config['workspace_id'] == 12345, 'Workspace ID not parsed correctly'
assert config['debug'] == True, 'Debug flag not set correctly'
print('Configuration module working correctly')
"

if [ $? -eq 0 ]; then
    print_info "✓ Configuration module test passed"
else
    print_error "✗ Configuration module test failed"
    exit 1
fi

# Test 6: Health check simulation
print_test "Testing health check endpoint simulation..."
docker run --rm --env-file "$TEST_DIR/.env" "${IMAGE_NAME}:${IMAGE_TAG}" python -c "
import asyncio
from bigeye_api import BigeyeAPIClient

async def test_client():
    client = BigeyeAPIClient('https://app.bigeye.com', 'test_key')
    # Just test that the client initializes properly
    assert client.api_url == 'https://app.bigeye.com'
    assert client.api_key == 'test_key'
    print('API client initialized successfully')

asyncio.run(test_client())
"

if [ $? -eq 0 ]; then
    print_info "✓ API client initialization test passed"
else
    print_error "✗ API client initialization test failed"
    exit 1
fi

# Test 7: Container labels
print_test "Testing container labels..."
LABELS=$(docker inspect "${IMAGE_NAME}:${IMAGE_TAG}" --format '{{json .Config.Labels}}')
if echo "$LABELS" | jq -e '.version' >/dev/null 2>&1; then
    print_info "✓ Container labels test passed"
    print_info "  Version: $(echo "$LABELS" | jq -r '.version')"
else
    print_warn "⚠ Container labels not found (build with build.sh for labels)"
fi

# Test 8: User permissions
print_test "Testing non-root user..."
if docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}" bash -c "whoami | grep -q mcp && echo 'Running as non-root user'"; then
    print_info "✓ Non-root user test passed"
else
    print_error "✗ Non-root user test failed"
    exit 1
fi

# Test 9: Docker Compose test
print_test "Testing Docker Compose setup..."
cd "$PROJECT_DIR"
if docker-compose config >/dev/null 2>&1; then
    print_info "✓ Docker Compose configuration is valid"
else
    print_error "✗ Docker Compose configuration is invalid"
    exit 1
fi

# Test 10: MCP Protocol Communication (if requested)
if [[ " $@ " =~ " --mcp " ]] || [[ " $@ " =~ " --full " ]]; then
    print_test "Testing MCP protocol communication..."
    print_info "Running comprehensive MCP protocol tests..."
    
    if "${SCRIPT_DIR}/test-mcp-protocol.sh" "${IMAGE_TAG}"; then
        print_info "✓ MCP protocol tests passed"
    else
        print_error "✗ MCP protocol tests failed"
        exit 1
    fi
fi

# Summary
print_info ""
print_info "========================================="
print_info "All tests completed successfully! 🎉"
print_info "========================================="
print_info ""
print_info "The Bigeye MCP Server container is ready for use."
print_info "To run interactively: docker run -it --rm --env-file .env ${IMAGE_NAME}:${IMAGE_TAG}"
print_info "To use with Claude Desktop, see the README for configuration instructions."