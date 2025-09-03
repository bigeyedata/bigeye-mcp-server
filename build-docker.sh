#!/bin/bash

# Build script for Bigeye MCP Server Docker image
# This creates an ephemeral Docker image for use with Claude Desktop

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default image name and tag
IMAGE_NAME="bigeye-mcp-server"
IMAGE_TAG="latest"

echo -e "${GREEN}Building Bigeye MCP Server Docker image...${NC}"
echo "Image: ${IMAGE_NAME}:${IMAGE_TAG}"

# Build the Docker image
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Docker image built successfully!${NC}"
    echo ""
    echo -e "${YELLOW}To use this image with Claude Desktop, add the following to your config:${NC}"
    echo -e "${YELLOW}Location: ~/Library/Application Support/Claude/claude_desktop_config.json${NC}"
    echo ""
    cat << EOF
{
  "mcpServers": {
    "bigeye": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "BIGEYE_API_KEY=<your-api-key>",
        "-e",
        "BIGEYE_API_URL=<your-bigeye-url>",
        "-e",
        "BIGEYE_WORKSPACE_ID=<your-workspace-id>",
        "-e",
        "BIGEYE_DEBUG=false",
        "${IMAGE_NAME}:${IMAGE_TAG}"
      ]
    }
  }
}
EOF
    echo ""
    echo -e "${GREEN}Remember to replace the placeholder values with your actual Bigeye credentials!${NC}"
else
    echo -e "${RED}❌ Docker build failed!${NC}"
    exit 1
fi