#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# start.sh — Launch all AstroLlama components in separate terminal windows.
#
# Works on macOS and Linux.  Requires pwsh (PowerShell 7+) on PATH.
#
# Usage:
#   ./start.sh [OPTIONS]
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

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --llama-port)   LLAMA_PORT="$2";   shift 2 ;;
        --mcp-port)     MCP_PORT="$2";     shift 2 ;;
        --client-port)  CLIENT_PORT="$2";  shift 2 ;;
        --no-delay)     NO_DELAY=true;     shift   ;;
        -h|--help)
            sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

LLAMA_SCRIPT="$DIR/run_llama.ps1"
MCP_SCRIPT="$DIR/run_mcp.ps1"
CLIENT_SCRIPT="$DIR/run_client.ps1"

for script in "$LLAMA_SCRIPT" "$MCP_SCRIPT" "$CLIENT_SCRIPT"; do
    [[ -f "$script" ]] || { echo "ERROR: Script not found: $script" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# Verify pwsh is available
# ---------------------------------------------------------------------------
if ! command -v pwsh &>/dev/null; then
    echo "ERROR: 'pwsh' (PowerShell 7+) not found on PATH." >&2
    echo "       Install from https://github.com/PowerShell/PowerShell/releases" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# open_window <title> <script> [extra args...]
#   Opens the given .ps1 script in a new terminal window/tab.
# ---------------------------------------------------------------------------
open_window() {
    local title="$1"
    local script="$2"
    shift 2
    local extra_args=("$@")

    local pwsh_cmd="pwsh -NoLogo -File \"$script\""
    for arg in "${extra_args[@]+"${extra_args[@]}"}"; do
        pwsh_cmd+=" $arg"
    done

    local os
    os="$(uname)"

    if [[ "$os" == "Darwin" ]]; then
        # macOS — open a new Terminal.app window via AppleScript.
        local shell_cmd="cd \"$DIR\" ; $pwsh_cmd"
        # Escape double-quotes for AppleScript string literals.
        local escaped_cmd="${shell_cmd//\"/\\\"}"
        osascript -e "tell application \"Terminal\" to do script \"$escaped_cmd\""

    else
        # Linux — probe common terminal emulators.
        if command -v gnome-terminal &>/dev/null; then
            gnome-terminal --title="$title" -- pwsh -NoLogo -File "$script" "${extra_args[@]+"${extra_args[@]}"}" &

        elif command -v konsole &>/dev/null; then
            konsole --title "$title" -- pwsh -NoLogo -File "$script" "${extra_args[@]+"${extra_args[@]}"}" &

        elif command -v xfce4-terminal &>/dev/null; then
            xfce4-terminal --title="$title" -x pwsh -NoLogo -File "$script" "${extra_args[@]+"${extra_args[@]}"}" &

        elif command -v lxterminal &>/dev/null; then
            lxterminal --title="$title" -e "$pwsh_cmd" &

        elif command -v xterm &>/dev/null; then
            xterm -title "$title" -e "$pwsh_cmd" &

        else
            echo "WARNING: No GUI terminal emulator found. '$title' will run in the background."
            eval "$pwsh_cmd" &
        fi
    fi
}

# ---------------------------------------------------------------------------
# Launch components
# ---------------------------------------------------------------------------
echo ""
echo "  AstroLlama Launcher"
echo "  ==================="
echo ""

echo "  [1/3] llama-server  ->  http://127.0.0.1:${LLAMA_PORT}"
open_window "AstroLlama - llama-server" "$LLAMA_SCRIPT" -Port "$LLAMA_PORT"

[[ "$NO_DELAY" == "true" ]] || sleep 1.5

echo "  [2/3] MCP server    ->  http://0.0.0.0:${MCP_PORT}/mcp"
open_window "AstroLlama - MCP Server" "$MCP_SCRIPT" -Port "$MCP_PORT"

[[ "$NO_DELAY" == "true" ]] || sleep 1.5

echo "  [3/3] Client        ->  http://localhost:${CLIENT_PORT}"
open_window "AstroLlama - Client" "$CLIENT_SCRIPT" -Port "$CLIENT_PORT"

echo ""
echo "  All components launched in separate windows."
echo ""
