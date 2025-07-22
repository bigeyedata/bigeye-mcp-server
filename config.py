"""
Configuration module for Bigeye MCP Server

Loads configuration from environment variables with fallbacks to config.json file.
This module supports the same authentication approach as the main branch while
also allowing dynamic authentication through the chat interface.
"""

import os
import json
import sys
from typing import Dict, Any, Optional

# Path to the configuration file
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")

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
        print(f"[BIGEYE MCP CONFIG] ERROR: Missing required environment variables: {', '.join(missing_vars)}", file=sys.stderr)
        print("[BIGEYE MCP CONFIG] Please create a .env file with the required variables. See .env.example for reference.", file=sys.stderr)
        sys.exit(1)

def load_config_from_file() -> Dict[str, Any]:
    """Load configuration from JSON file."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        else:
            print(f"[BIGEYE MCP CONFIG] Warning: Config file {CONFIG_FILE} not found", file=sys.stderr)
            return {}
    except Exception as e:
        print(f"[BIGEYE MCP CONFIG] Error loading config file: {str(e)}", file=sys.stderr)
        return {}

# Load configuration from file
file_config = load_config_from_file()

# Create the configuration with environment variable overrides
config = {
    # API URL (env var with BIGEYE_BASE_URL taking precedence, config file, or default)
    "api_url": os.environ.get("BIGEYE_BASE_URL", 
                              os.environ.get("BIGEYE_API_URL", 
                                             file_config.get("api_url", DEFAULT_CONFIG["api_url"]))),
    
    # API Key (env var, config file, or None)
    "api_key": os.environ.get("BIGEYE_API_KEY", file_config.get("api_key", DEFAULT_CONFIG["api_key"])),
    
    # Workspace ID (env var, config file, or None)
    "workspace_id": None,  # Will be set below with proper error handling
    
    # Debug mode (env var, config file, or False)
    "debug": os.environ.get("BIGEYE_DEBUG", "").lower() in ["true", "1", "yes"] 
        if "BIGEYE_DEBUG" in os.environ 
        else file_config.get("debug", DEFAULT_CONFIG["debug"])
}

# Handle workspace_id conversion with proper error handling
workspace_id_str = os.environ.get("BIGEYE_WORKSPACE_ID") or file_config.get("workspace_id")
if workspace_id_str:
    try:
        # Only convert if we have a non-empty string
        if str(workspace_id_str).strip():
            config["workspace_id"] = int(workspace_id_str)
    except ValueError:
        print(f"[BIGEYE MCP CONFIG] Warning: Invalid workspace_id value: {workspace_id_str}", file=sys.stderr)
        config["workspace_id"] = None

# Check required environment variables
check_required_env_vars()

# Log the configuration (without API key for security)
if config["debug"]:
    safe_config = config.copy()
    if safe_config["api_key"]:
        safe_config["api_key"] = "***********"
    print(f"[BIGEYE MCP CONFIG] Loaded configuration: {json.dumps(safe_config, indent=2)}", file=sys.stderr)