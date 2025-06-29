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
    "workspace_id": int(os.environ.get("BIGEYE_WORKSPACE_ID", file_config.get("workspace_id", 0))) or None,
    
    # Debug mode (env var, config file, or False)
    "debug": os.environ.get("BIGEYE_DEBUG", "").lower() in ["true", "1", "yes"] 
        if "BIGEYE_DEBUG" in os.environ 
        else file_config.get("debug", DEFAULT_CONFIG["debug"])
}

# Log the configuration (without API key for security)
if config["debug"]:
    safe_config = config.copy()
    if safe_config["api_key"]:
        safe_config["api_key"] = "***********"
    print(f"[BIGEYE MCP CONFIG] Loaded configuration: {json.dumps(safe_config, indent=2)}", file=sys.stderr)