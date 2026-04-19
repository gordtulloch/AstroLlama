#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Restart all AstroLlama components.

.DESCRIPTION
    Stops any running llama-server, MCP server, and FastAPI client processes,
    then relaunches them via start.ps1 in separate windows.

.PARAMETER RepoRoot
    Path to the repository root.  Defaults to the directory containing this script.

.PARAMETER LlamaPort
    Port for the llama-server.  Defaults to 8081.

.PARAMETER McpPort
    Port for the MCP server.  Defaults to 8000.

.PARAMETER ClientPort
    Port for the FastAPI client.  Defaults to 8080.

.PARAMETER NoDelay
    Skip the brief pause between launching each component.

.EXAMPLE
    .\restart.ps1
    .\restart.ps1 -LlamaPort 8082
#>
param(
    [string]$RepoRoot  = $PSScriptRoot,
    [int]$LlamaPort    = 8081,
    [int]$McpPort      = 8000,
    [int]$ClientPort   = 8080,
    [switch]$NoDelay
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Helper: stop processes matching a WMI/CIM command-line substring.
# Returns the count of processes killed.
# ---------------------------------------------------------------------------
function Stop-ByCommandLine {
    param(
        [string]$Match,
        [string]$Label
    )

    $killed = 0

    if ($IsWindows -or $env:OS -eq "Windows_NT") {
        $procs = Get-CimInstance Win32_Process |
            Where-Object { $_.CommandLine -and $_.CommandLine -like "*$Match*" }
        foreach ($p in $procs) {
            try {
                Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
                $killed++
            } catch { }
        }
    } else {
        # macOS / Linux — use pgrep / pkill
        $pids = & pgrep -f ([regex]::Escape($Match)) 2>/dev/null
        foreach ($pid in $pids) {
            if ($pid) {
                & kill -TERM $pid 2>/dev/null
                $killed++
            }
        }
    }

    if ($killed -gt 0) {
        Write-Host "  Stopped $killed $Label process(es)." -ForegroundColor DarkYellow
    } else {
        Write-Host "  No running $Label processes found." -ForegroundColor DarkGray
    }

    return $killed
}

# ---------------------------------------------------------------------------
# Helper: stop all processes with a given executable name (Windows exe name).
# ---------------------------------------------------------------------------
function Stop-ByName {
    param(
        [string]$WinName,
        [string]$UnixMatch,
        [string]$Label
    )

    $killed = 0

    if ($IsWindows -or $env:OS -eq "Windows_NT") {
        $procs = Get-Process -Name $WinName -ErrorAction SilentlyContinue
        foreach ($p in $procs) {
            try { $p | Stop-Process -Force; $killed++ } catch { }
        }
    } else {
        $pids = & pgrep -f ([regex]::Escape($UnixMatch)) 2>/dev/null
        foreach ($pid in $pids) {
            if ($pid) { & kill -TERM $pid 2>/dev/null; $killed++ }
        }
    }

    if ($killed -gt 0) {
        Write-Host "  Stopped $killed $Label process(es)." -ForegroundColor DarkYellow
    } else {
        Write-Host "  No running $Label processes found." -ForegroundColor DarkGray
    }

    return $killed
}

# ---------------------------------------------------------------------------
# Stop phase
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "  AstroLlama Restart" -ForegroundColor Cyan
Write-Host "  ==================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Stopping components ..." -ForegroundColor Yellow
Write-Host ""

# llama-server (native binary)
$null = Stop-ByName -WinName "llama-server" -UnixMatch "llama-server" -Label "llama-server"

# MCP server (Python running mcp_server/server.py)
$null = Stop-ByCommandLine -Match "mcp_server/server.py"  -Label "MCP server"
$null = Stop-ByCommandLine -Match "mcp_server\server.py"  -Label "MCP server"  # Windows path variant

# FastAPI / uvicorn client (app.main:app)
$null = Stop-ByCommandLine -Match "app.main:app" -Label "FastAPI client"

# Close the PowerShell host windows opened by start.ps1 (-NoExit).
$null = Stop-ByCommandLine -Match "run_llama.ps1"   -Label "llama-server window"
$null = Stop-ByCommandLine -Match "run_mcp.ps1"     -Label "MCP server window"
$null = Stop-ByCommandLine -Match "run_client.ps1"  -Label "client window"

# Give OS a moment to release ports before restarting.
Write-Host ""
Write-Host "  Waiting for ports to be released ..." -ForegroundColor DarkGray
Start-Sleep -Seconds 2

# ---------------------------------------------------------------------------
# Restart phase — delegate to start.ps1
# ---------------------------------------------------------------------------
$StartScript = Join-Path $RepoRoot "start.ps1"
if (-not (Test-Path $StartScript)) {
    throw "start.ps1 not found at: $StartScript"
}

Write-Host "  Launching components ..." -ForegroundColor Yellow
Write-Host ""

$startArgs = @{
    LlamaPort  = $LlamaPort
    McpPort    = $McpPort
    ClientPort = $ClientPort
}
if ($NoDelay) { $startArgs['NoDelay'] = $true }

& $StartScript @startArgs
