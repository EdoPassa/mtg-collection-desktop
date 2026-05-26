param(
    [Parameter(Mandatory = $true)]
    [string]$Version
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

if ($Version -match '^v') {
    $Version = $Version.Substring(1)
}

$wailsPath = Join-Path $repoRoot "wails.json"
$wails = Get-Content $wailsPath -Raw | ConvertFrom-Json
if (-not $wails.info) {
    throw "wails.json is missing an info section"
}
$wails.info.productVersion = $Version
$wails | ConvertTo-Json -Depth 10 | Set-Content $wailsPath -Encoding utf8

$packagePath = Join-Path $repoRoot "frontend\package.json"
$package = Get-Content $packagePath -Raw | ConvertFrom-Json
$package.version = $Version
$package | ConvertTo-Json -Depth 10 | Set-Content $packagePath -Encoding utf8

Write-Host "Set release version to $Version"
