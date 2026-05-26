#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot,
    [Parameter(Mandatory = $true)]
    [string]$Domain,
    [Parameter(Mandatory = $true)]
    [string]$Email,
    [switch]$Staging,
    [switch]$Renew
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "certs/letsencrypt") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "certs/lib") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "certs/log") | Out-Null

$args = @(
    "run", "--rm", "-it",
    "-v", "${RepoRoot}/certs/letsencrypt:/etc/letsencrypt",
    "-v", "${RepoRoot}/certs/lib:/var/lib/letsencrypt",
    "-v", "${RepoRoot}/certs/log:/var/log/letsencrypt",
    "certbot/certbot"
)

if ($Renew) {
    $args += @("renew")
} else {
    $args += @(
        "certonly",
        "--manual",
        "--preferred-challenges", "dns",
        "--agree-tos",
        "--manual-public-ip-logging-ok",
        "--email", $Email,
        "--domain", $Domain,
        "--key-type", "rsa",
        "--rsa-key-size", "4096"
    )
    if ($Staging) {
        $args += "--staging"
    }
}

Write-Host "Starting Certbot container..." -ForegroundColor Cyan
Write-Host "For Register.ca: add the TXT value shown for _acme-challenge.$Domain, then continue when prompted." -ForegroundColor Yellow

& docker @args
if ($LASTEXITCODE -ne 0) {
    throw "Certbot command failed"
}

$certPath = "certs/letsencrypt/live/$Domain/fullchain.pem"
$keyPath = "certs/letsencrypt/live/$Domain/privkey.pem"

Write-Host "Certificate flow completed." -ForegroundColor Green
Write-Host "Set these in .env:" -ForegroundColor DarkGray
Write-Host "APP_TLS_ENABLED=true" -ForegroundColor DarkGray
Write-Host "APP_TLS_CERTFILE=/$certPath" -ForegroundColor DarkGray
Write-Host "APP_TLS_KEYFILE=/$keyPath" -ForegroundColor DarkGray
