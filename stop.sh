#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# stop.sh — Stop all running AstroLlama components.
#
# Usage:
#   ./stop.sh
# -----------------------------------------------------------------------------
set -euo pipefail

stop_matching() {
    local pattern="$1"
    local label="$2"
    local count=0
    local self_pid=$$

    while IFS= read -r pid; do
        [[ -z "$pid" || "$pid" == "$self_pid" ]] && continue
        kill -TERM "$pid" 2>/dev/null && (( count++ )) || true
    done < <(pgrep -f "$pattern" 2>/dev/null || true)

    if (( count > 0 )); then
        printf "  Stopped %d %s process(es).\n" "$count" "$label"
    else
        printf "  No running %s processes found.\n" "$label"
    fi
}

echo ""
echo "  AstroLlama Stop"
echo "  ==============="
echo ""

stop_matching "llama-server"      "llama-server"
stop_matching "mcp_server/server" "MCP server"
stop_matching "app.main:app"      "FastAPI client"

echo ""
echo "  All components stopped."
echo ""
