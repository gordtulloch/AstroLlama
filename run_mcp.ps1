#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Start the MCP server (./mcp_server/server.py).

.DESCRIPTION
    Activates the virtual environment (creating it if needed), installs
    dependencies, and launches the MCP server.

.PARAMETER RepoRoot
    Path to the repository root.  Defaults to the directory containing this script.

.PARAMETER VenvDir
    Path to the Python virtual environment.  Defaults to .venv inside the repo.

.EXAMPLE
    .\run_mcp.ps1
#>
param(
    [string]$RepoRoot = $PSScriptRoot,
    [string]$VenvDir  = (Join-Path $PSScriptRoot ".venv"),
    [int]$Port        = 8000
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

# ---- Virtual-environment setup -------------------------------------------
$PythonExe = if ($IsWindows -or $env:OS -eq "Windows_NT") {
    Join-Path $VenvDir "Scripts\python.exe"
} else {
    Join-Path $VenvDir "bin/python"
}

if (-not (Test-Path $PythonExe)) {
    Write-Host "Creating virtual environment at $VenvDir ..." -ForegroundColor Cyan
    $PyCmd = @("python", "python3", "py") | Where-Object { Get-Command $_ -ErrorAction SilentlyContinue } | Select-Object -First 1
    if (-not $PyCmd) { throw "No Python interpreter found. Install Python 3.11+ and ensure it is on PATH." }
    & $PyCmd -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { throw "Failed to create virtual environment." }
}

Write-Host "Installing / verifying dependencies ..." -ForegroundColor Cyan
& $PythonExe -m pip install --quiet --upgrade pip
& $PythonExe -m pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

# ---- Launch MCP server ---------------------------------------------------
$McpDir = Join-Path $RepoRoot "mcp_server"
if (-not (Test-Path (Join-Path $McpDir "server.py"))) {
    throw "MCP server not found at $McpDir\server.py"
}

Write-Host ""
Write-Host "  MCP Server" -ForegroundColor Green
Write-Host "  http://localhost:${Port}/mcp" -ForegroundColor Green
Write-Host ""

Set-Location $RepoRoot
& $PythonExe -m mcp_server.server --http $Port
Set-Location ..
