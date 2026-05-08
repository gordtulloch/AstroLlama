#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot,
    [int]$Port = 8081,
    [string]$ModelFile = "Llama-3.2-1B.Q8_0.gguf",
    [int]$ContextSize = 8192,
    [int]$NgLayers = 99
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

$env:LLAMA_PORT = $Port.ToString()
$env:LLAMA_MODEL_FILE = $ModelFile
$env:LLAMA_CTX_SIZE = $ContextSize.ToString()
$env:LLAMA_NGL = $NgLayers.ToString()

docker compose up -d llama
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up for llama failed"
}

Write-Host "llama.cpp server available at http://localhost:${Port}" -ForegroundColor Green
Write-Host "Model: ${ModelFile}" -ForegroundColor DarkGray
