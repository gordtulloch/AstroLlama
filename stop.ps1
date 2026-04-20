#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Stop all running AstroLlama components.

.DESCRIPTION
    Terminates the llama-server, MCP server, and FastAPI client processes.

.EXAMPLE
    .\stop.ps1
#>

$ErrorActionPreference = "Stop"

function Stop-ByCommandLine {
    param([string]$Match, [string]$Label)
    $killed = 0
    if ($IsWindows -or $env:OS -eq "Windows_NT") {
        $procs = Get-CimInstance Win32_Process |
            Where-Object { $_.CommandLine -and $_.CommandLine -like "*$Match*" }
        foreach ($p in $procs) {
            try { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue; $killed++ } catch { }
        }
    } else {
        $pids = & pgrep -f ([regex]::Escape($Match)) 2>/dev/null
        foreach ($pid in $pids) {
            if ($pid) { & kill -TERM $pid 2>/dev/null; $killed++ }
        }
    }
    if ($killed -gt 0) {
        Write-Host "  Stopped $killed $Label process(es)." -ForegroundColor DarkYellow
    } else {
        Write-Host "  No running $Label processes found." -ForegroundColor DarkGray
    }
}

function Stop-ByName {
    param([string]$WinName, [string]$UnixMatch, [string]$Label)
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
}

Write-Host ""
Write-Host "  AstroLlama Stop" -ForegroundColor Cyan
Write-Host "  ===============" -ForegroundColor Cyan
Write-Host ""

Stop-ByName        -WinName "llama-server" -UnixMatch "llama-server"   -Label "llama-server"
Stop-ByCommandLine -Match "mcp_server/server.py"  -Label "MCP server"
Stop-ByCommandLine -Match "mcp_server\server.py"  -Label "MCP server"
Stop-ByCommandLine -Match "mcp_server.server"     -Label "MCP server"
Stop-ByCommandLine -Match "app.main:app"           -Label "FastAPI client"

# Close the PowerShell host windows that were opened by start.ps1 (-NoExit).
# Each window's command-line contains the name of the run script it launched.
Stop-ByCommandLine -Match "run_llama.ps1"   -Label "llama-server window"
Stop-ByCommandLine -Match "run_mcp.ps1"     -Label "MCP server window"
Stop-ByCommandLine -Match "run_client.ps1"  -Label "client window"

Write-Host ""
Write-Host "  All components stopped." -ForegroundColor Cyan
Write-Host ""
