#!/bin/bash
set -euo pipefail

# Script to build Bigeye MCP Server Docker image with proper versioning
# Usage: ./build.sh [version] [--push]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
IMAGE_NAME="bigeye-mcp-server"
REGISTRY="${DOCKER_REGISTRY:-}"  # Set DOCKER_REGISTRY env var if using a registry

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

# Get version from argument or git
if [ $# -ge 1 ] && [ "$1" != "--push" ]; then
    VERSION="$1"
else
    # Generate version from git
    GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "no-git")
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "no-branch")
    VERSION="${GIT_BRANCH}-${GIT_COMMIT}"
    
    # Check for uncommitted changes
    if ! git diff --quiet 2>/dev/null; then
        VERSION="${VERSION}-dirty"
        print_warn "Uncommitted changes detected, appending '-dirty' to version"
    fi
fi

# Check if we should push
PUSH_TO_REGISTRY=false
if [[ " $@ " =~ " --push " ]]; then
    PUSH_TO_REGISTRY=true
    if [ -z "$REGISTRY" ]; then
        print_error "DOCKER_REGISTRY environment variable must be set when using --push"
        exit 1
    fi
fi

print_info "Building Bigeye MCP Server Docker image..."
print_info "Version: $VERSION"
print_info "Project directory: $PROJECT_DIR"

# Change to project directory
cd "$PROJECT_DIR"

# Create build timestamp
BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build the image with version tags
print_info "Building Docker image..."

# Build with build arguments
docker build \
    --build-arg BUILD_VERSION="$VERSION" \
    --build-arg BUILD_TIMESTAMP="$BUILD_TIMESTAMP" \
    --build-arg GIT_COMMIT="$GIT_COMMIT" \
    --label "version=$VERSION" \
    --label "build.timestamp=$BUILD_TIMESTAMP" \
    --label "git.commit=$GIT_COMMIT" \
    --label "git.branch=$GIT_BRANCH" \
    -t "${IMAGE_NAME}:latest" \
    -t "${IMAGE_NAME}:${VERSION}" \
    .

if [ $? -eq 0 ]; then
    print_info "Build successful!"
    print_info "Tagged as:"
    print_info "  - ${IMAGE_NAME}:latest"
    print_info "  - ${IMAGE_NAME}:${VERSION}"
else
    print_error "Build failed!"
    exit 1
fi

# Also tag with registry prefix if registry is set
if [ -n "$REGISTRY" ]; then
    print_info "Tagging for registry: $REGISTRY"
    docker tag "${IMAGE_NAME}:latest" "${REGISTRY}/${IMAGE_NAME}:latest"
    docker tag "${IMAGE_NAME}:${VERSION}" "${REGISTRY}/${IMAGE_NAME}:${VERSION}"
    
    print_info "Registry tags:"
    print_info "  - ${REGISTRY}/${IMAGE_NAME}:latest"
    print_info "  - ${REGISTRY}/${IMAGE_NAME}:${VERSION}"
fi

# Push to registry if requested
if [ "$PUSH_TO_REGISTRY" = true ]; then
    print_info "Pushing to registry: $REGISTRY"
    
    docker push "${REGISTRY}/${IMAGE_NAME}:latest"
    docker push "${REGISTRY}/${IMAGE_NAME}:${VERSION}"
    
    if [ $? -eq 0 ]; then
        print_info "Push successful!"
    else
        print_error "Push failed!"
        exit 1
    fi
fi

# Show image details
print_info "Image details:"
docker images --filter "reference=${IMAGE_NAME}" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

# Show build labels
print_info "Build labels:"
docker inspect "${IMAGE_NAME}:${VERSION}" --format '{{json .Config.Labels}}' | jq .

print_info "Build complete!"