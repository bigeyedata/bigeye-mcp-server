"""
Configuration module for Bigeye MCP Server

Loads configuration from environment variables passed by Claude Desktop.
No fallbacks - credentials must be provided via environment variables.
"""

import os
import sys
from typing import Dict, Any, Optional

# Default configuration
DEFAULT_CONFIG = {
    "api_url": "https://app.bigeye.com",
    "api_key": None,
    "workspace_id": None,
    "debug": False
}

# Check if we have required environment variables
def check_required_env_vars():
    """Check if required environment variables are set."""
    required_vars = ["BIGEYE_API_KEY", "BIGEYE_WORKSPACE_ID"]
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("\n[BIGEYE MCP CONFIG] ERROR: Missing required environment variables", file=sys.stderr)
        print("="*60, file=sys.stderr)
        print("\nThe Bigeye MCP server requires credentials to be configured in your", file=sys.stderr)
        print("Claude Desktop configuration file.", file=sys.stderr)
        print("\nTo set up your credentials:", file=sys.stderr)
        print("\n1. Open your Claude Desktop config file:", file=sys.stderr)
        print("   macOS: ~/Library/Application Support/Claude/claude_desktop_config.json", file=sys.stderr)
        print("   Windows: %APPDATA%\\Claude\\claude_desktop_config.json", file=sys.stderr)
        print("\n2. Add or update the Bigeye server configuration:", file=sys.stderr)
        print("""
{
  "mcpServers": {
    "bigeye": {
      "command": "python",
      "args": ["/path/to/bigeye-mcp-server/server.py"],
      "env": {
        "BIGEYE_API_KEY": "<your-api-key>",
        "BIGEYE_API_URL": "<your-bigeye-url>",
        "BIGEYE_WORKSPACE_ID": "<your-workspace-id>",
        "BIGEYE_DEBUG": "false"
      }
    }
  }
}""", file=sys.stderr)
        print("\n3. Replace the placeholder values:", file=sys.stderr)
        print("   - BIGEYE_API_KEY: Generate in Bigeye (Settings > API Keys)", file=sys.stderr)
        print("   - BIGEYE_API_URL: Your Bigeye instance URL (e.g., https://app.bigeye.com)", file=sys.stderr)
        print("   - BIGEYE_WORKSPACE_ID: Found in Bigeye URL or Settings", file=sys.stderr)
        print("\n4. Save the file and restart Claude Desktop", file=sys.stderr)
        print("\n" + "="*60, file=sys.stderr)
        print(f"\nMissing variables: {', '.join(missing_vars)}", file=sys.stderr)
        sys.exit(1)


# Create the configuration from environment variables only
config = {
    # API URL (env var with BIGEYE_BASE_URL taking precedence or BIGEYE_API_URL)
    "api_url": os.environ.get("BIGEYE_BASE_URL", 
                              os.environ.get("BIGEYE_API_URL", 
                                             DEFAULT_CONFIG["api_url"])),
    
    # API Key (env var only)
    "api_key": os.environ.get("BIGEYE_API_KEY"),
    
    # Workspace ID (env var only)
    "workspace_id": None,  # Will be set below with proper error handling
    
    # Debug mode (env var only)
    "debug": os.environ.get("BIGEYE_DEBUG", "").lower() in ["true", "1", "yes"]
}

# Handle workspace_id conversion with proper error handling
workspace_id_str = os.environ.get("BIGEYE_WORKSPACE_ID")
if workspace_id_str:
    try:
        # Only convert if we have a non-empty string
        if str(workspace_id_str).strip():
            config["workspace_id"] = int(workspace_id_str)
    except ValueError:
        print(f"[BIGEYE MCP CONFIG] ERROR: Invalid workspace_id value: {workspace_id_str}", file=sys.stderr)
        print("[BIGEYE MCP CONFIG] The workspace_id must be a number.", file=sys.stderr)
        sys.exit(1)

# Check required environment variables
check_required_env_vars()

# Log the configuration (without API key for security)
if config["debug"]:
    safe_config = config.copy()
    if safe_config["api_key"]:
        safe_config["api_key"] = "***" + safe_config["api_key"][-4:] if len(safe_config["api_key"]) > 4 else "****"
    print(f"[BIGEYE MCP CONFIG] Loaded configuration:", file=sys.stderr)
    print(f"  API URL: {safe_config['api_url']}", file=sys.stderr)
    print(f"  API Key: {safe_config['api_key']}", file=sys.stderr)
    print(f"  Workspace ID: {safe_config['workspace_id']}", file=sys.stderr)
    print(f"  Debug: {safe_config['debug']}", file=sys.stderr)