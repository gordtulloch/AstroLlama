#!/usr/bin/env pwsh
param(
    [ValidateSet("cli", "server")]
    [string]$Mode = "server",

    [string]$RepoRoot = $PSScriptRoot,
    [string]$CudaBin = "C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.8\bin",
    [string]$BinDir,
    [string]$ModelPath,

    [string]$Prompt = "Hello",
    [int]$Predict = 128,

    [string]$ListenHost = "127.0.0.1",
    [int]$Port = 8081,
    [int]$ContextSize = 8192,

    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"

$DevLogDir = Join-Path $RepoRoot "logs\dev"
$LlamaLogPath = Join-Path $DevLogDir "llama_server.log"

function Write-LlamaLog {
    param([string]$Message)

    New-Item -ItemType Directory -Force -Path $DevLogDir | Out-Null
    Add-Content -Path $LlamaLogPath -Value $Message -Encoding utf8
}

function Get-DotEnvValue {
    param(
        [string]$EnvFilePath,
        [string]$Key
    )

    if (-not (Test-Path $EnvFilePath)) {
        return $null
    }

    foreach ($line in Get-Content -Path $EnvFilePath) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }

        if ($trimmed -match "^$([regex]::Escape($Key))\s*=\s*(.*)$") {
            return $matches[1].Trim().Trim('"').Trim("'")
        }
    }

    return $null
}

function Resolve-LlamaBinDir {
    param(
        [string]$RepoRoot,
        [string]$RequestedBinDir,
        [string]$RequiredExe
    )

    if ($RequestedBinDir) {
        $requestedExe = Join-Path $RequestedBinDir $RequiredExe
        if (-not (Test-Path $requestedExe)) {
            throw "Requested bin dir does not contain ${RequiredExe}: $RequestedBinDir"
        }
        return $RequestedBinDir
    }

    $envFilePath = Join-Path $RepoRoot ".env"
    $llamaCppPath = Get-DotEnvValue -EnvFilePath $envFilePath -Key "LLAMA_CPP_PATH"

    $candidates = @(
        $llamaCppPath,
        (Join-Path $RepoRoot "ai\bin"),
        (Join-Path $PSScriptRoot "ai\bin")
    )

    foreach ($candidate in $candidates) {
        if (-not (Test-Path $candidate)) {
            continue
        }

        if (Test-Path (Join-Path $candidate $RequiredExe)) {
            return $candidate
        }
    }

    throw "Could not locate $RequiredExe in candidate folders. Checked: $($candidates -join ', ')"
}

if (-not $ModelPath) {
    $envFilePath = Join-Path $RepoRoot ".env"
    $modelPathFromEnv = Get-DotEnvValue -EnvFilePath $envFilePath -Key "MODEL_PATH"
    if ($modelPathFromEnv) {
        $ModelPath = $modelPathFromEnv
    }
}

if (-not $ModelPath) {
    $aiDir = Join-Path $RepoRoot "ai"
    $preferredModels = @(
        (Join-Path $aiDir "Llama-3.2-1B.Q8_0.gguf"),
        (Join-Path $aiDir "Qwen2.5-3B-Instruct-Q8_0.gguf"),
        (Join-Path $aiDir "mistral-7b-instruct-v0.2.Q3_K_M.gguf"),
        (Join-Path $aiDir "deepseek-coder.gguf")
    )

    $firstExistingPreferred = $preferredModels | Where-Object { Test-Path $_ } | Select-Object -First 1
    if ($firstExistingPreferred) {
        $ModelPath = $firstExistingPreferred
    }
    else {
        $anyGguf = Get-ChildItem -Path $aiDir -Filter "*.gguf" -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

        if ($anyGguf) {
            $ModelPath = $anyGguf.FullName
        }
    }
}

$requiredExe = if ($Mode -eq "server") { "llama-server.exe" } else { "llama-cli.exe" }
$binDir = Resolve-LlamaBinDir -RepoRoot $RepoRoot -RequestedBinDir $BinDir -RequiredExe $requiredExe

$cudaAvailable = Test-Path $CudaBin

if (-not (Test-Path $ModelPath)) {
    throw "Model file not found: $ModelPath"
}

if ($cudaAvailable) {
    if ($env:PATH -notlike "$CudaBin*") {
        $env:PATH = "$CudaBin;$env:PATH"
    }
    Write-Host "CUDA runtime found. Attempting GPU-capable execution." -ForegroundColor DarkGray
    Write-LlamaLog "CUDA runtime found. Attempting GPU-capable execution."
}
else {
    Write-Warning "CUDA bin folder not found at '$CudaBin'. Continuing in CPU mode."
    Write-LlamaLog "WARNING: CUDA bin folder not found at '$CudaBin'. Continuing in CPU mode."
}

Push-Location $binDir
try {
    if ($Mode -eq "cli") {
        $exe = Join-Path $binDir "llama-cli.exe"
        if (-not (Test-Path $exe)) {
            throw "Executable not found: $exe"
        }

        $llamaArgs = @(
            "-m", $ModelPath,
            "-p", $Prompt,
            "-n", $Predict.ToString(),
            "--no-display-prompt",
            "--flash-attn", "auto",
            "-ngl", "32"
        ) + $ExtraArgs

        Write-Host "Starting llama-cli from: $exe" -ForegroundColor Cyan
        Write-Host "Using model: $ModelPath" -ForegroundColor DarkGray
        Write-LlamaLog "Starting llama-cli from: $exe"
        Write-LlamaLog "Using model: $ModelPath"
        & $exe @llamaArgs
        $exitCode = $LASTEXITCODE
        if ($exitCode -ne 0) {
            $hint = if ($exitCode -eq -1073741515) {
                "Likely missing runtime DLL dependency (commonly CUDA DLLs)."
            }
            else {
                ""
            }
            Write-LlamaLog "ERROR: llama-cli exited with code $exitCode. $hint"
            throw "llama-cli exited with code $exitCode. $hint"
        }
        Write-LlamaLog "llama-cli exited cleanly."
        exit 0
    }

    $exe = Join-Path $binDir "llama-server.exe"
    if (-not (Test-Path $exe)) {
        throw "Executable not found: $exe"
    }

    $llamaArgs = @(
        "-m", $ModelPath,
        "--host", $ListenHost,
        "--port", $Port.ToString(),
        "--ctx-size", $ContextSize.ToString(),
        "--flash-attn", "auto",
        "-ngl", "32"
    ) + $ExtraArgs

    Write-Host "Starting llama-server from: $exe" -ForegroundColor Cyan
    Write-Host "Using model: $ModelPath" -ForegroundColor DarkGray
    Write-Host "URL: http://$ListenHost`:$Port" -ForegroundColor Green
    Write-LlamaLog "Starting llama-server from: $exe"
    Write-LlamaLog "Using model: $ModelPath"
    Write-LlamaLog "URL: http://$ListenHost`:$Port"
    & $exe @llamaArgs
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        $hint = if ($exitCode -eq -1073741515) {
            "Likely missing runtime DLL dependency (commonly CUDA DLLs)."
        }
        else {
            ""
        }
        Write-LlamaLog "ERROR: llama-server exited with code $exitCode. $hint"
        throw "llama-server exited with code $exitCode. $hint"
    }
    Write-LlamaLog "llama-server exited cleanly."
    exit 0
}
finally {
    Pop-Location
}