#!/usr/bin/env pwsh
param(
    [string]$RepoRoot = $PSScriptRoot,
    [string]$Domain = "astrollama.openastronomy.ca",
    [string]$LanIp = "",
    [int]$Days = 825,
    [string]$OpenSslImage = "astrollama-app:latest"
)

$ErrorActionPreference = "Stop"
Set-Location $RepoRoot

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH."
}

if (-not $LanIp) {
    $LanIp = (Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object {
            $_.IPAddress -notlike '169.254.*' -and
            $_.InterfaceAlias -notmatch 'Loopback' -and
            $_.PrefixOrigin -ne 'WellKnown'
        } |
        Sort-Object SkipAsSource, InterfaceMetric |
        Select-Object -First 1 -ExpandProperty IPAddress)
}
if (-not $LanIp) { $LanIp = "127.0.0.1" }

New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "certs/selfsigned") | Out-Null

$opensslCmd = "openssl req -x509 -newkey rsa:4096 -sha256 -days $Days -nodes -keyout /work/certs/selfsigned/astrollama.key -out /work/certs/selfsigned/astrollama.crt -subj /CN=$Domain -addext subjectAltName=DNS:$Domain,DNS:localhost,IP:127.0.0.1,IP:$LanIp"

docker run --rm -v "${RepoRoot}:/work" $OpenSslImage sh -c $opensslCmd
if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate self-signed certificate via container image: $OpenSslImage"
}

Write-Host "Created certs/selfsigned/astrollama.crt and certs/selfsigned/astrollama.key" -ForegroundColor Green
Write-Host "SAN includes DNS:$Domain and IP:$LanIp" -ForegroundColor DarkGray
Write-Host "Enable HTTPS by setting APP_TLS_ENABLED=true in .env" -ForegroundColor DarkGray
