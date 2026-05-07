<#
.SYNOPSIS
  Runs the README §5-2 scenarios (S1~S4) against a server and dumps each
  scenario's JSON summary into <OutDir>\<Label>-<ScenarioId>.json.

.DESCRIPTION
  By default the script also boots and tears down a release-build server on
  127.0.0.1:9000. Pass -KeepServer to skip that and use a server you already
  started yourself.

.EXAMPLE
  .\bench\run-scenarios.ps1 -Label baseline
  .\bench\run-scenarios.ps1 -Label improved -OutDir bench\results
  .\bench\run-scenarios.ps1 -Label baseline -Scenarios S1,S2 -KeepServer
#>

param(
  [Parameter(Mandatory = $true)]
  [string]$Label,

  [string]$OutDir = "bench\results",

  [string]$Addr = "127.0.0.1:9000",

  # Skip S1~S4 selection. Default = all four.
  [string[]]$Scenarios = @("S1", "S2", "S3", "S4"),

  # If set, the script assumes a server is already running at -Addr.
  [switch]$KeepServer
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$scenarioTable = @{
  "S1" = @{ Clients = 100; Rate = 1;  Duration = 60  }
  "S2" = @{ Clients = 500; Rate = 1;  Duration = 60  }
  "S3" = @{ Clients = 500; Rate = 10; Duration = 30  }
  "S4" = @{ Clients = 500; Rate = 2;  Duration = 600 }
}

foreach ($id in $Scenarios) {
  if (-not $scenarioTable.ContainsKey($id)) {
    throw "Unknown scenario: $id (expected one of S1, S2, S3, S4)"
  }
}

if (-not (Test-Path $OutDir)) {
  New-Item -ItemType Directory -Path $OutDir | Out-Null
}

Write-Host "[bench] building release binaries..." -ForegroundColor Cyan
& cargo build --release -p server -p loadtest
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }

$serverExe   = Join-Path $repoRoot "target\release\server.exe"
$loadtestExe = Join-Path $repoRoot "target\release\loadtest.exe"

if (-not (Test-Path $serverExe))   { throw "server binary not found: $serverExe" }
if (-not (Test-Path $loadtestExe)) { throw "loadtest binary not found: $loadtestExe" }

$serverProc = $null
if (-not $KeepServer) {
  Write-Host "[bench] starting server at $Addr ..." -ForegroundColor Cyan
  $serverLog = Join-Path $OutDir "server-$Label.log"
  $serverProc = Start-Process -FilePath $serverExe `
    -ArgumentList @("--addr", $Addr) `
    -RedirectStandardOutput $serverLog `
    -RedirectStandardError  ($serverLog + ".err") `
    -PassThru -NoNewWindow

  # Wait for the listener to come up. cargo target dir is warm so this is fast,
  # but give it a few seconds in case of a cold start.
  $hostPart, $portPart = $Addr -split ":"
  $deadline = (Get-Date).AddSeconds(15)
  $ready = $false
  while ((Get-Date) -lt $deadline) {
    try {
      $tcp = New-Object System.Net.Sockets.TcpClient
      $tcp.Connect($hostPart, [int]$portPart)
      $tcp.Close()
      $ready = $true
      break
    } catch {
      Start-Sleep -Milliseconds 300
    }
  }
  if (-not $ready) {
    if (-not $serverProc.HasExited) { $serverProc | Stop-Process -Force }
    throw "server failed to come up at $Addr within 15s. Check $serverLog"
  }
  Write-Host "[bench] server up (pid=$($serverProc.Id))" -ForegroundColor Green
}

try {
  foreach ($id in $Scenarios) {
    $sc = $scenarioTable[$id]
    $outFile = Join-Path $OutDir "$Label-$id.json"

    Write-Host ""
    Write-Host "[bench] $id : $($sc.Clients) clients, $($sc.Rate) msg/s, $($sc.Duration)s -> $outFile" -ForegroundColor Yellow

    # The Rust binary writes the JSON summary to stdout; tracing logs go to
    # stderr and are intentionally not captured into the JSON file.
    & $loadtestExe `
        --addr     $Addr `
        --clients  $sc.Clients `
        --rate     $sc.Rate `
        --duration $sc.Duration `
        --output   json `
        --label    "$Label-$id" `
      | Out-File -FilePath $outFile -Encoding utf8

    if ($LASTEXITCODE -ne 0) {
      throw "loadtest scenario $id failed (exit $LASTEXITCODE)"
    }

    # Brief gap so the server can reset broadcast lag counters etc.
    Start-Sleep -Seconds 2
  }
}
finally {
  if ($null -ne $serverProc -and -not $serverProc.HasExited) {
    Write-Host "[bench] stopping server (pid=$($serverProc.Id))" -ForegroundColor Cyan
    $serverProc | Stop-Process -Force
  }
}

Write-Host ""
Write-Host "[bench] done. Results in: $OutDir" -ForegroundColor Green
Get-ChildItem $OutDir -Filter "$Label-*.json" | Select-Object Name, Length
