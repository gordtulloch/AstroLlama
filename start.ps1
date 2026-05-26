#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot,
    [int]$LlamaPort = 8081,
    [int]$McpPort = 8000,
    [int]$ClientPort = 8080,
    [switch]$NoDelay
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

$env:LLAMA_PORT = $LlamaPort.ToString()
$env:MCP_PORT = $McpPort.ToString()
$env:APP_PORT = $ClientPort.ToString()

Write-Host ""
Write-Host "  AstroLlama Docker Launcher" -ForegroundColor Cyan
Write-Host "  =========================" -ForegroundColor Cyan
Write-Host ""

Write-Host "  Starting containers (llama, mcp, app)..." -ForegroundColor Green

docker compose up -d --build
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed"
}

Write-Host ""
Write-Host "  Services started:" -ForegroundColor Cyan
Write-Host "  llama:  http://localhost:${LlamaPort}" -ForegroundColor Green
Write-Host "  mcp:    http://localhost:${McpPort}/mcp" -ForegroundColor Green
Write-Host "  client: http://localhost:${ClientPort}" -ForegroundColor Green
Write-Host "  client (TLS): https://localhost:${ClientPort} (when APP_TLS_ENABLED=true)" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  Use 'docker compose logs -f' to tail all logs." -ForegroundColor DarkGray
Write-Host ""