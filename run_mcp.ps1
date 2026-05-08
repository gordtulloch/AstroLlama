#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot,
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

$env:MCP_PORT = $Port.ToString()

docker compose up -d --build mcp
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up for mcp failed"
}

Write-Host "MCP server available at http://localhost:${Port}/mcp" -ForegroundColor Green
