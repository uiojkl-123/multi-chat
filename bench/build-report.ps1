<#
.SYNOPSIS
  Builds a markdown report comparing two label sets of loadtest JSON results
  (e.g. baseline vs improved) produced by run-scenarios.ps1.

.EXAMPLE
  .\bench\build-report.ps1 -Baseline baseline -Improved improved
  .\bench\build-report.ps1 -Baseline baseline -Improved improved `
                           -InDir bench\results `
                           -OutFile bench\results\report.md
#>

param(
  [Parameter(Mandatory = $true)] [string]$Baseline,
  [Parameter(Mandatory = $true)] [string]$Improved,

  [string]$InDir   = "bench\results",
  [string]$OutFile = "bench\results\report.md",

  [string[]]$Scenarios = @("S1", "S2", "S3", "S4")
)

$ErrorActionPreference = "Stop"

$scenarioMeta = @{
  "S1" = "기준 동작 확인 (100 clients, 1 msg/s, 60s)"
  "S2" = "요구사항: 500명 동시 접속 (500 clients, 1 msg/s, 60s)"
  "S3" = "스트레스 (500 clients, 10 msg/s, 30s)"
  "S4" = "번인 (500 clients, 2 msg/s, 600s)"
}

function Read-Summary([string]$path) {
  if (-not (Test-Path $path)) { return $null }
  $raw = Get-Content $path -Raw -Encoding UTF8
  if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
  return $raw | ConvertFrom-Json
}

function Format-Cell($base, $imp, [string]$prop, [string]$fmt = "{0}") {
  $b = if ($null -ne $base) { $base.$prop } else { $null }
  $i = if ($null -ne $imp)  { $imp.$prop  } else { $null }

  $bs = if ($null -eq $b) { "-" } else { $fmt -f $b }
  $is = if ($null -eq $i) { "-" } else { $fmt -f $i }

  $delta = "-"
  if ($null -ne $b -and $null -ne $i) {
    try {
      $bn = [double]$b
      $inum = [double]$i
      $d = $inum - $bn
      if ($bn -ne 0) {
        $pct = ($d / $bn) * 100.0
        $delta = "{0:+0.0;-0.0;0} / {1:+0.0;-0.0;0}%" -f $d, $pct
      } else {
        $delta = "{0:+0.0;-0.0;0}" -f $d
      }
    } catch {
      $delta = "-"
    }
  }
  return @($bs, $is, $delta)
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$now = Get-Date -Format "yyyy-MM-dd HH:mm"

$lines = @()
$lines += "# 부하 테스트 결과 보고서"
$lines += ""
$lines += "- 생성 시각: $now"
$lines += "- Baseline 라벨: ``$Baseline``"
$lines += "- Improved 라벨: ``$Improved``"
$lines += "- 결과 디렉터리: ``$InDir``"
$lines += ""
$lines += "> 각 시나리오는 README §5-2의 정의를 따른다. 지표 정의는 README §5-1 참고."
$lines += ""

# Summary table — one row per scenario, key metrics only.
$lines += "## 요약 (시나리오별 핵심 지표)"
$lines += ""
$lines += "| 시나리오 | P50 (ms) Δ | P95 (ms) Δ | P99 (ms) Δ | Loss (%) Δ | 무결성 오류 Δ |"
$lines += "|----------|------------|------------|------------|------------|---------------|"

foreach ($id in $Scenarios) {
  $base = Read-Summary (Join-Path $InDir "$Baseline-$id.json")
  $imp  = Read-Summary (Join-Path $InDir "$Improved-$id.json")

  $p50 = Format-Cell $base $imp "latency_p50_ms"
  $p95 = Format-Cell $base $imp "latency_p95_ms"
  $p99 = Format-Cell $base $imp "latency_p99_ms"
  $loss = Format-Cell $base $imp "loss_rate_pct" "{0:N2}"
  $err = Format-Cell $base $imp "integrity_errors"

  $row = "| $id | $($p50[0]) → $($p50[1])  ($($p50[2])) | $($p95[0]) → $($p95[1])  ($($p95[2])) | $($p99[0]) → $($p99[1])  ($($p99[2])) | $($loss[0]) → $($loss[1])  ($($loss[2])) | $($err[0]) → $($err[1]) |"
  $lines += $row
}

$lines += ""

# Per-scenario detail tables.
foreach ($id in $Scenarios) {
  $base = Read-Summary (Join-Path $InDir "$Baseline-$id.json")
  $imp  = Read-Summary (Join-Path $InDir "$Improved-$id.json")

  $title = $scenarioMeta[$id]
  if (-not $title) { $title = $id }

  $lines += "## $id — $title"
  $lines += ""

  if ($null -eq $base -and $null -eq $imp) {
    $lines += "_결과 파일을 찾을 수 없음: ``$InDir\$Baseline-$id.json``, ``$InDir\$Improved-$id.json``_"
    $lines += ""
    continue
  }

  $lines += "| 측정 | Clients | Rate | Duration | Sent | Received | Expected | Loss (%) | P50 (ms) | P95 (ms) | P99 (ms) | 오류 |"
  $lines += "|------|---------|------|----------|------|----------|----------|----------|----------|----------|----------|------|"

  function FormatRow($label, $s) {
    if ($null -eq $s) {
      return "| $label | - | - | - | - | - | - | - | - | - | - | - |"
    }
    return ("| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7:N2} | {8} | {9} | {10} | {11} |" -f `
      $label, $s.clients, $s.rate, $s.duration, $s.total_sent, $s.total_received,
      $s.expected_received, $s.loss_rate_pct, $s.latency_p50_ms, $s.latency_p95_ms, $s.latency_p99_ms,
      $s.integrity_errors)
  }

  $lines += (FormatRow "Baseline ($Baseline)" $base)
  $lines += (FormatRow "Improved ($Improved)" $imp)
  $lines += ""
}

$outDirParent = Split-Path -Parent $OutFile
if ($outDirParent -and -not (Test-Path $outDirParent)) {
  New-Item -ItemType Directory -Path $outDirParent | Out-Null
}

$lines | Out-File -FilePath $OutFile -Encoding utf8
Write-Host "[bench] report written: $OutFile" -ForegroundColor Green
