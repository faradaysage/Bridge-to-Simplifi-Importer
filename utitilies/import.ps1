<#
import.ps1

Usage:
  ./import.ps1 "VenmoStatement*.csv"

Behavior:
  - Expands wildcard internally (PowerShell-safe)
  - Runs bridge once per input CSV
  - Writes one output CSV per input
  - Merges all outputs into merged-import.csv

Assumptions:
  - This script lives in repo in ./utilities
  - bridge script already exists
  - csv-merge.py exists in repo root
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $true, Position = 0)]
  [string]$InputPattern
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info  ($m) { Write-Host "[INFO]  $m" -ForegroundColor Cyan }
function Write-Ok    ($m) { Write-Host "[OK]    $m" -ForegroundColor Green }
function Write-Warn  ($m) { Write-Host "[WARN]  $m" -ForegroundColor Yellow }
function Write-Fail  ($m) { Write-Host "[FAIL]  $m" -ForegroundColor Red }
function Write-Title ($m) { Write-Host "`n=== $m ===" -ForegroundColor Magenta }

try {
  Write-Title "Bridge Import (Wildcard Batch)"

  $rootDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
  $python   = "python"
  $bridgePy = Join-Path $rootDir "..\bridge-to-simplifi.py"
  $mergePy  = Join-Path $rootDir "csv-merge.py"

  if (-not (Test-Path $bridgePy)) { throw "Missing bridge script: $bridgePy" }
  if (-not (Test-Path $mergePy))  { throw "Missing CSV merger:  $mergePy" }

  # Expand wildcard explicitly (always an array)
  $inputs = @(Get-ChildItem -Path $InputPattern -File | Sort-Object Name)

  if ($inputs.Count -eq 0) {
    throw "No files matched pattern: $InputPattern"
  }

  Write-Info "Matched $($inputs.Count) file(s)"

  $outputs = @()

  foreach ($file in $inputs) {
    Write-Title "Processing $($file.Name)"

    $suffix = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
    $outName = "import_$suffix.csv"
    $outPath = Join-Path $file.DirectoryName $outName

    Write-Info "Input:  $($file.FullName)"
    Write-Info "Output: $outPath"

    $args = @(
      $bridgePy,
      "--import", $file.FullName,
      "--export", $outPath
    )

    Write-Host "`n> $python $($args -join ' ')" -ForegroundColor DarkGray

    & $python @args
    if ($LASTEXITCODE -ne 0) {
      throw "Bridge failed for $($file.Name)"
    }

    if (-not (Test-Path $outPath)) {
      throw "Expected output not found: $outPath"
    }

    $outputs += $outPath
    Write-Ok "Wrote $outPath"
  }

  Write-Title "Merging CSV outputs"

  $mergedPath = Join-Path (Get-Location) "merged-import.csv"

  & $python $mergePy $mergedPath @outputs
  if ($LASTEXITCODE -ne 0) {
    throw "CSV merge failed"
  }

  Write-Ok "Merged CSV written to: $mergedPath"
}
catch {
  Write-Fail $_.Exception.Message
  exit 1
}
