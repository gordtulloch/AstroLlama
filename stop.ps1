#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

Write-Host ""
Write-Host "  AstroLlama Docker Stop" -ForegroundColor Cyan
Write-Host "  =====================" -ForegroundColor Cyan
Write-Host ""

docker compose down --remove-orphans
if ($LASTEXITCODE -ne 0) {
    throw "docker compose down failed"
}

Write-Host ""
Write-Host "  All AstroLlama containers stopped." -ForegroundColor Green
Write-Host ""
