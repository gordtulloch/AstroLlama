#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot,
    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

$env:APP_PORT = $Port.ToString()

docker compose up -d --build app
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up for app failed"
}

Write-Host "Client available at http://localhost:${Port}" -ForegroundColor Green
