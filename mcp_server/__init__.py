"""
MCP - Astronomical Data Access via Model Context Protocol

A modular MCP server for accessing astronomical datasets with automatic
data management and analysis tools.
"""

import json
from pathlib import Path

__version__ = "0.1.0"

def _load_base_dir() -> Path:
    config_path = Path(__file__).parent / "mcp_config.json"
    try:
        with open(config_path) as f:
            config = json.load(f)
        cwd = config.get("mcpServers", {}).get("mcp_server", {}).get("cwd")
        if cwd:
            return Path(cwd)
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return Path(__file__).parent

BASE_DIR = _load_base_dir()
