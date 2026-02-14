#!/bin/bash

# Bigeye MCP Server Docker Container Management Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PROJECT_NAME="bigeye-mcp"
CONTAINER_NAME="bigeye-mcp-server"
IMAGE_NAME="bigeye-mcp-server"
IMAGE_TAG="latest"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

function print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

function print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

function print_service() {
    echo -e "${CYAN}[SERVICE]${NC} $1"
}

function start_containers() {
    print_status "Starting Bigeye MCP Server container..."
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d bigeye-mcp
    sleep 2
    print_status "Container started successfully!"
    echo ""
    status
}

function stop_containers() {
    print_status "Stopping Bigeye MCP Server container..."
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" stop
    print_status "Container stopped"
}

function restart_containers() {
    print_status "Restarting Bigeye MCP Server container..."
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" restart
    print_status "Container restarted"
}

function status() {
    print_status "Container status:"
    echo ""

    if docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "  ${GREEN}●${NC} ${CONTAINER_NAME} - running"
    elif docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "  ${RED}●${NC} ${CONTAINER_NAME} - stopped"
    else
        echo -e "  ${YELLOW}○${NC} ${CONTAINER_NAME} - not created"
    fi

    echo ""
    if docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Image}}" | grep -E "(NAMES|${CONTAINER_NAME})" || true
    fi
}

function logs() {
    print_status "Showing container logs (Ctrl+C to exit)..."
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" logs -f bigeye-mcp
}

function rebuild() {
    print_status "Rebuilding Bigeye MCP Server Docker image..."
    docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" -t "bigeye-mcp-ephemeral:latest" "${SCRIPT_DIR}"
    print_status "Removing old container..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
    print_status "Recreating container..."
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d bigeye-mcp
    print_status "Rebuild complete"
}

function clean() {
    print_warning "This will remove the container and its volumes"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" down -v
        print_status "Container and volumes removed"
    else
        print_status "Cancelled"
    fi
}

function show_help() {
    echo "Bigeye MCP Server Container Management"
    echo ""
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  start              - Start the container"
    echo "  stop               - Stop the container"
    echo "  restart            - Restart the container"
    echo "  status             - Show container status"
    echo "  logs               - Show container logs (follow mode)"
    echo "  rebuild            - Rebuild image and recreate container"
    echo "  clean              - Remove container and volumes"
    echo "  help               - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start           - Start the MCP server container"
    echo "  $0 rebuild         - Rebuild image and restart"
    echo "  $0 logs            - Follow container logs"
}

# Main script logic
case "${1:-}" in
    start)
        start_containers
        ;;
    stop)
        stop_containers
        ;;
    restart)
        restart_containers
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    rebuild)
        rebuild
        ;;
    clean)
        clean
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        if [ -n "${1:-}" ]; then
            print_error "Unknown command: $1"
        fi
        show_help
        exit 1
        ;;
esac
