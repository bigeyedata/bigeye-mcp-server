"""
Configuration module for Bigeye MCP Server

Loads configuration from environment variables.
No fallbacks - credentials must be provided via environment variables.
"""

import os
import sys
import time
from pathlib import Path


# Default configuration
DEFAULT_CONFIG = {
    "api_url": "https://app.bigeye.com",
    "api_key": None,
    "workspace_id": None,
    "debug": False
}

# Known typos/aliases -> correct variable name
_TYPO_MAP = {
    "BIGEYE_URL": "BIGEYE_BASE_URL",
    "BIGEYE_KEY": "BIGEYE_API_KEY",
    "BIGEYE_WS_ID": "BIGEYE_WORKSPACE_ID",
    "BIGEYE_WORKSPACE": "BIGEYE_WORKSPACE_ID",
}


def _detect_runtime() -> str:
    """Detect whether we're running inside Docker or Claude Desktop."""
    if Path("/.dockerenv").exists() or os.environ.get("container"):
        return "docker"
    return "claude-desktop"


def _redact_api_key(key: str | None) -> str:
    """Return a redacted version of the API key for display."""
    if not key:
        return "NOT SET"
    if len(key) > 16:
        return key[:12] + "..." + key[-4:]
    if len(key) > 4:
        return "***" + key[-4:]
    return "****"


def _print_startup_banner(cfg: dict) -> None:
    """Print a concise config status summary to stderr."""
    api_key_display = _redact_api_key(cfg.get("api_key"))
    base_url = cfg.get("api_url") or "NOT SET"
    ws_id = cfg.get("workspace_id")
    ws_display = str(ws_id) if ws_id is not None else "NOT SET"
    debug_display = str(cfg.get("debug", False)).lower()

    print("[BIGEYE MCP] Configuration Status:", file=sys.stderr)
    print(f"  BIGEYE_API_KEY:      {api_key_display}", file=sys.stderr)
    print(f"  BIGEYE_BASE_URL:     {base_url}", file=sys.stderr)
    print(f"  BIGEYE_WORKSPACE_ID: {ws_display}", file=sys.stderr)
    print(f"  BIGEYE_DEBUG:        {debug_display}", file=sys.stderr)


def _check_typos() -> None:
    """Warn about common env var typos/misspellings."""
    for typo, correct in _TYPO_MAP.items():
        if os.environ.get(typo) and not os.environ.get(correct):
            print(
                f"[BIGEYE MCP] WARNING: Found '{typo}' — did you mean '{correct}'?",
                file=sys.stderr,
            )


def check_required_env_vars() -> None:
    """Check if required environment variables are set."""
    required_vars = ["BIGEYE_API_KEY", "BIGEYE_WORKSPACE_ID"]
    missing_vars = [v for v in required_vars if not os.environ.get(v)]

    if not missing_vars:
        return

    runtime = _detect_runtime()
    missing_str = ", ".join(missing_vars)

    print(
        f"\n[BIGEYE MCP] ERROR: Missing required environment variables: {missing_str}",
        file=sys.stderr,
    )

    if runtime == "docker":
        print(
            "\n  Add the missing variables to your .env file or docker-compose environment section.\n"
            "  Required: BIGEYE_API_KEY, BIGEYE_WORKSPACE_ID\n"
            "  Optional: BIGEYE_BASE_URL (default: https://app.bigeye.com), BIGEYE_DEBUG\n",
            file=sys.stderr,
        )
        print(
            "  Container will stay idle. Fix configuration and restart.",
            file=sys.stderr,
        )
        # Sleep instead of exiting so the container stays "Up" and admins can
        # read logs without a restart loop flooding them.
        try:
            time.sleep(3600)
        except KeyboardInterrupt:
            pass
        sys.exit(1)
    else:
        print(
            "\n  Add the missing variables to your Claude Desktop config:\n"
            "    macOS: ~/Library/Application Support/Claude/claude_desktop_config.json\n"
            "    Windows: %APPDATA%\\Claude\\claude_desktop_config.json\n"
            "\n"
            "  Required env vars: BIGEYE_API_KEY, BIGEYE_WORKSPACE_ID\n"
            "  Optional: BIGEYE_BASE_URL (default: https://app.bigeye.com), BIGEYE_DEBUG\n",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Module-level initialization
# ---------------------------------------------------------------------------

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
        if str(workspace_id_str).strip():
            config["workspace_id"] = int(workspace_id_str)
    except ValueError:
        print(f"[BIGEYE MCP] ERROR: Invalid workspace_id value: {workspace_id_str}", file=sys.stderr)
        print("[BIGEYE MCP] The workspace_id must be a number.", file=sys.stderr)
        sys.exit(1)

# Startup diagnostics (always printed)
_print_startup_banner(config)
_check_typos()

# Validate required variables
check_required_env_vars()
