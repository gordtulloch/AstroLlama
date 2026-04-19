#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Start the LocalAI Chat Client (FastAPI + Uvicorn).

.DESCRIPTION
    Activates the virtual environment (creating it if needed), installs
    dependencies, and launches the server.  The llama-server should already
    be running before you start this script.

.PARAMETER RepoRoot
    Path to the repository root.  Defaults to the directory containing this script.

.PARAMETER VenvDir
    Path to the Python virtual environment.  Defaults to .venv inside the repo.

.PARAMETER Host
    Host to bind to.  Defaults to localhost.

.PARAMETER Port
    Port to listen on.  Defaults to 8080.

.PARAMETER Reload
    Pass -Reload to enable uvicorn's hot-reload (development mode).

.EXAMPLE
    .\run_client.ps1
    .\run_client.ps1 -Reload
    .\run_client.ps1 -Port 9090
#>
param(
    [string]$RepoRoot = $PSScriptRoot,
    [string]$VenvDir  = (Join-Path $PSScriptRoot ".venv"),
    [string]$AppHost  = "localhost",
    [int]$Port        = 8080,
    [switch]$Reload
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
    # Locate a usable Python interpreter
    $PyCmd = @("python", "python3", "py") | Where-Object { Get-Command $_ -ErrorAction SilentlyContinue } | Select-Object -First 1
    if (-not $PyCmd) { throw "No Python interpreter found. Install Python 3.11+ and ensure it is on PATH." }
    & $PyCmd -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { throw "Failed to create virtual environment." }
}

Write-Host "Installing / verifying dependencies ..." -ForegroundColor Cyan
& $PythonExe -m pip install --quiet --upgrade pip
& $PythonExe -m pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

# ---- .env setup ----------------------------------------------------------
$EnvFile = Join-Path $RepoRoot ".env"
if (-not (Test-Path $EnvFile)) {
    Write-Warning ".env file not found - copying .env.example. Edit it before running in production."
    Copy-Item (Join-Path $RepoRoot ".env.example") $EnvFile
}

# ---- Launch server -------------------------------------------------------
$UvicornArgs = @(
    "-m", "uvicorn",
    "app.main:app",
    "--host", $AppHost,
    "--port", $Port.ToString()
)

if ($Reload) {
    $UvicornArgs += "--reload"
    Write-Host "Hot-reload enabled." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  LocalAI Chat Client" -ForegroundColor Green
Write-Host "  http://${AppHost}:${Port}" -ForegroundColor Green
Write-Host ""

& $PythonExe @UvicornArgs
