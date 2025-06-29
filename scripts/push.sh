#!/bin/bash
set -euo pipefail

# Script to push Bigeye MCP Server Docker image to registry
# Usage: ./push.sh [version] [registry]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="bigeye-mcp-server"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Parse arguments
VERSION="${1:-latest}"
REGISTRY="${2:-${DOCKER_REGISTRY:-}}"

# Check if registry is provided
if [ -z "$REGISTRY" ]; then
    print_error "Registry not specified. Please provide a registry as second argument or set DOCKER_REGISTRY environment variable"
    print_info "Usage: $0 [version] [registry]"
    print_info "Example: $0 v1.0.0 ghcr.io/myorg"
    exit 1
fi

# Check if image exists locally
if ! docker image inspect "${IMAGE_NAME}:${VERSION}" >/dev/null 2>&1; then
    print_error "Image ${IMAGE_NAME}:${VERSION} not found locally"
    print_info "Please build the image first with: ./build.sh ${VERSION}"
    exit 1
fi

print_info "Pushing Bigeye MCP Server to registry"
print_info "Image: ${IMAGE_NAME}:${VERSION}"
print_info "Registry: ${REGISTRY}"

# Tag image for registry if not already tagged
REGISTRY_IMAGE="${REGISTRY}/${IMAGE_NAME}"
print_info "Tagging image for registry..."
docker tag "${IMAGE_NAME}:${VERSION}" "${REGISTRY_IMAGE}:${VERSION}"

# Also tag as latest if pushing latest
if [ "$VERSION" = "latest" ] || [ "$1" = "" ]; then
    docker tag "${IMAGE_NAME}:${VERSION}" "${REGISTRY_IMAGE}:latest"
fi

# Login check
print_info "Checking registry authentication..."
if ! docker pull "${REGISTRY}/hello-world" >/dev/null 2>&1; then
    print_warn "Cannot pull from registry. You may need to login first."
    print_info "Run: docker login ${REGISTRY}"
fi

# Push the image
print_info "Pushing ${REGISTRY_IMAGE}:${VERSION}..."
if docker push "${REGISTRY_IMAGE}:${VERSION}"; then
    print_info "✓ Successfully pushed ${REGISTRY_IMAGE}:${VERSION}"
else
    print_error "✗ Failed to push ${REGISTRY_IMAGE}:${VERSION}"
    exit 1
fi

# Push latest tag if applicable
if [ "$VERSION" = "latest" ] || [ "$1" = "" ]; then
    print_info "Pushing ${REGISTRY_IMAGE}:latest..."
    if docker push "${REGISTRY_IMAGE}:latest"; then
        print_info "✓ Successfully pushed ${REGISTRY_IMAGE}:latest"
    else
        print_error "✗ Failed to push ${REGISTRY_IMAGE}:latest"
        exit 1
    fi
fi

# Show pushed images
print_info ""
print_info "Pushed images:"
print_info "  - ${REGISTRY_IMAGE}:${VERSION}"
if [ "$VERSION" = "latest" ] || [ "$1" = "" ]; then
    print_info "  - ${REGISTRY_IMAGE}:latest"
fi

# Generate pull commands
print_info ""
print_info "To pull this image:"
print_info "  docker pull ${REGISTRY_IMAGE}:${VERSION}"
print_info ""
print_info "To use with Claude Desktop, update your config with:"
print_info "  \"command\": \"docker\","
print_info "  \"args\": ["
print_info "    \"run\", \"--rm\", \"-i\","
print_info "    \"--env-file\", \"/path/to/.env\","
print_info "    \"${REGISTRY_IMAGE}:${VERSION}\""
print_info "  ]"