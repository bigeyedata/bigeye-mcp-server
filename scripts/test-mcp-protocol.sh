#!/bin/bash
set -euo pipefail

# Script to test MCP protocol communication with the Bigeye MCP Server Docker container
# Usage: ./test-mcp-protocol.sh [image_tag] [--debug]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="bigeye-mcp-server"
IMAGE_TAG="${1:-latest}"
DEBUG_MODE=false

# Check for debug flag
if [[ " $@ " =~ " --debug " ]]; then
    DEBUG_MODE=true
fi

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

# Check Python is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is required but not installed"
    exit 1
fi

print_info "Testing MCP Protocol Communication"
print_info "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
print_info "Test mode: ${DEBUG_MODE}"

# Change to project directory
cd "$PROJECT_DIR"

# Run the Python test client
print_test "Running MCP protocol tests..."

DEBUG_FLAG=""
if [ "$DEBUG_MODE" = true ]; then
    DEBUG_FLAG="--debug"
fi

# Run the test client
python3 tests/mcp_test_client.py \
    --image "${IMAGE_NAME}:${IMAGE_TAG}" \
    --fixtures tests/fixtures/mcp_messages.json \
    $DEBUG_FLAG

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    print_info "All MCP protocol tests passed! ✓"
else
    print_error "Some MCP protocol tests failed! ✗"
    exit $TEST_EXIT_CODE
fi

# Additional manual tests for verification
print_info ""
print_test "Running additional verification tests..."

# Test 1: Quick container health check
print_test "Container health check..."
if docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}" python -c "import server; print('Server module loaded')"; then
    print_info "✓ Container health check passed"
else
    print_error "✗ Container health check failed"
    exit 1
fi

# Test 2: Verify MCP dependencies
print_test "MCP dependencies check..."
if docker run --rm "${IMAGE_NAME}:${IMAGE_TAG}" python -c "from mcp.server.fastmcp import FastMCP; print('MCP imported successfully')"; then
    print_info "✓ MCP dependencies check passed"
else
    print_error "✗ MCP dependencies check failed"
    exit 1
fi

print_info ""
print_info "========================================="
print_info "MCP Protocol Testing Complete! 🎉"
print_info "========================================="
print_info ""
print_info "The Bigeye MCP Server successfully:"
print_info "  ✓ Handles MCP initialization handshake"
print_info "  ✓ Lists available tools, resources, and prompts"
print_info "  ✓ Executes tool calls via JSON-RPC"
print_info "  ✓ Reads resources correctly"
print_info "  ✓ Handles errors gracefully"
print_info ""
print_info "Ready for integration with Claude Desktop, Cursor, or VS Code!"