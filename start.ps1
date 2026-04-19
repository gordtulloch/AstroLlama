#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Launch all AstroLlama components, each in its own terminal window.

.DESCRIPTION
    Starts llama-server, the MCP server, and the FastAPI client in separate
    terminal windows.  Works on Windows, macOS, and Linux (requires pwsh and
    a supported GUI terminal on Linux).

.PARAMETER RepoRoot
    Path to the repository root.  Defaults to the directory containing this
    script.

.PARAMETER LlamaPort
    Port for the llama-server.  Defaults to 8081.

.PARAMETER McpPort
    Port for the MCP server.  Defaults to 8000.

.PARAMETER ClientPort
    Port for the FastAPI client.  Defaults to 8080.

.PARAMETER NoDelay
    Skip the brief pause between launching each component.

.EXAMPLE
    .\start.ps1
    .\start.ps1 -NoDelay
#>
param(
    [string]$RepoRoot   = $PSScriptRoot,
    [int]$LlamaPort     = 8081,
    [int]$McpPort       = 8000,
    [int]$ClientPort    = 8080,
    [switch]$NoDelay
)

$ErrorActionPreference = "Stop"

$LlamaScript  = Join-Path $RepoRoot "run_llama.ps1"
$McpScript    = Join-Path $RepoRoot "run_mcp.ps1"
$ClientScript = Join-Path $RepoRoot "run_client.ps1"

foreach ($s in $LlamaScript, $McpScript, $ClientScript) {
    if (-not (Test-Path $s)) { throw "Script not found: $s" }
}

# Detect the PowerShell host executable to use when spawning child windows.
# Prefer pwsh (PS 7+) when available; fall back to powershell.exe on Windows.
$PwshExe = if (Get-Command pwsh -ErrorAction SilentlyContinue) {
    "pwsh"
} elseif ($IsWindows -or $env:OS -eq "Windows_NT") {
    "powershell.exe"
} else {
    throw "pwsh (PowerShell 7+) is required on macOS / Linux."
}

function Start-Component {
    param(
        [string]$Title,
        [string]$Script,
        [string[]]$ExtraArgs = @()
    )

    $allArgs = @("-NoLogo", "-File", $Script) + $ExtraArgs

    if ($IsWindows -or $env:OS -eq "Windows_NT") {
        # Build a -Command string that sets the window title then runs the file.
        $safeScript = $Script -replace "'", "''"
        $safeTitle  = $Title  -replace "'", "''"
        $cmd = "`$host.UI.RawUI.WindowTitle = '$safeTitle'; & '$safeScript'"
        if ($ExtraArgs) {
            # Quote values but NOT parameter names (tokens starting with '-').
            $argStr = ($ExtraArgs | ForEach-Object {
                if ($_ -match '^-') { $_ } else { "'$($_ -replace "'","''")'" }
            }) -join " "
            $cmd = "`$host.UI.RawUI.WindowTitle = '$safeTitle'; & '$safeScript' $argStr"
        }
        Start-Process $PwshExe -ArgumentList @("-NoExit", "-NoLogo", "-Command", $cmd)

    } elseif ($IsMacOS) {
        # Use Terminal.app via AppleScript.
        $safeScript = $Script -replace '"', '\"' -replace "'", "\'"
        $safeDir    = $RepoRoot -replace '"', '\"' -replace "'", "\'"
        $shellCmd   = "cd `"$safeDir`" ; $PwshExe -NoLogo -File `"$safeScript`""
        if ($ExtraArgs) { $shellCmd += " " + ($ExtraArgs -join " ") }
        $apple = "tell application `"Terminal`" to do script `"$shellCmd`""
        osascript -e $apple

    } else {
        # Linux: probe common terminal emulators.
        $launched = $false
        $emulators = @(
            @{ Exe = "gnome-terminal"; Args = { @("--title=$Title", "--") + @($PwshExe) + $allArgs } },
            @{ Exe = "konsole";        Args = { @("--title", $Title, "--", $PwshExe) + $allArgs } },
            @{ Exe = "xfce4-terminal"; Args = { @("--title=$Title", "-x", $PwshExe) + $allArgs } },
            @{ Exe = "lxterminal";     Args = { @("--title=$Title", "-e", "$PwshExe $($allArgs -join ' ')") } },
            @{ Exe = "xterm";          Args = { @("-title", $Title, "-e", "$PwshExe $($allArgs -join ' ')") } }
        )

        foreach ($em in $emulators) {
            if (-not (Get-Command $em.Exe -ErrorAction SilentlyContinue)) { continue }
            $termArgs = & $em.Args
            Start-Process $em.Exe -ArgumentList $termArgs
            $launched = $true
            break
        }

        if (-not $launched) {
            Write-Warning "No GUI terminal emulator found. '$Title' will run in the background (no window)."
            Start-Process $PwshExe -ArgumentList $allArgs
        }
    }
}

Write-Host ""
Write-Host "  AstroLlama Launcher" -ForegroundColor Cyan
Write-Host "  ===================" -ForegroundColor Cyan
Write-Host ""

Write-Host "  [1/3] llama-server  ->  http://localhost:${LlamaPort}" -ForegroundColor Green
Start-Component -Title "AstroLlama - llama-server" -Script $LlamaScript `
    -ExtraArgs @("-Port", $LlamaPort.ToString())

if (-not $NoDelay) { Start-Sleep -Milliseconds 1500 }

Write-Host "  [2/3] MCP server    ->  http://localhost:${McpPort}/mcp" -ForegroundColor Green
Start-Component -Title "AstroLlama - MCP Server" -Script $McpScript `
    -ExtraArgs @("-Port", $McpPort.ToString())

if (-not $NoDelay) { Start-Sleep -Milliseconds 1500 }

Write-Host "  [3/3] Client        ->  http://localhost:${ClientPort}" -ForegroundColor Green
Start-Component -Title "AstroLlama - Client" -Script $ClientScript `
    -ExtraArgs @("-Port", $ClientPort.ToString())

Write-Host ""
Write-Host "  All components launched in separate windows." -ForegroundColor Cyan
Write-Host ""
