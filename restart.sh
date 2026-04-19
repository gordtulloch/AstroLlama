#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# restart.sh — Stop all running AstroLlama components and restart them.
#
# Usage:
#   ./restart.sh [OPTIONS]
#
# Options:
#   --llama-port  PORT   llama-server port  (default: 8081)
#   --mcp-port    PORT   MCP server port    (default: 8000)
#   --client-port PORT   Client port        (default: 8080)
#   --no-delay           Skip pauses between launches
#   -h, --help           Show this message
# -----------------------------------------------------------------------------
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LLAMA_PORT=8081
MCP_PORT=8000
CLIENT_PORT=8080
NO_DELAY=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --llama-port)   LLAMA_PORT="$2";   shift 2 ;;
        --mcp-port)     MCP_PORT="$2";     shift 2 ;;
        --client-port)  CLIENT_PORT="$2";  shift 2 ;;
        --no-delay)     NO_DELAY=true;     shift   ;;
        -h|--help)
            sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Helper: kill processes matching a pattern; print a status line.
# ---------------------------------------------------------------------------
stop_matching() {
    local pattern="$1"
    local label="$2"
    local count=0

    # pgrep -f matches the full command line; skip our own PID.
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

# ---------------------------------------------------------------------------
# Stop phase
# ---------------------------------------------------------------------------
echo ""
echo "  AstroLlama Restart"
echo "  =================="
echo ""
echo "  Stopping components ..."
echo ""

stop_matching "llama-server"       "llama-server"
stop_matching "mcp_server/server"  "MCP server"
stop_matching "app.main:app"       "FastAPI client"

echo ""
echo "  Waiting for ports to be released ..."
sleep 2

# ---------------------------------------------------------------------------
# Restart phase — delegate to start.sh
# ---------------------------------------------------------------------------
START_SCRIPT="$DIR/start.sh"
if [[ ! -f "$START_SCRIPT" ]]; then
    echo "ERROR: start.sh not found at: $START_SCRIPT" >&2
    exit 1
fi

[[ -x "$START_SCRIPT" ]] || chmod +x "$START_SCRIPT"

echo "  Launching components ..."
echo ""

START_ARGS=(
    --llama-port  "$LLAMA_PORT"
    --mcp-port    "$MCP_PORT"
    --client-port "$CLIENT_PORT"
)
[[ "$NO_DELAY" == "true" ]] && START_ARGS+=(--no-delay)

"$START_SCRIPT" "${START_ARGS[@]}"
