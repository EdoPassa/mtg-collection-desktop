param(
    [string]$Version = "",
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Get-Command wails -ErrorAction SilentlyContinue)) {
    throw "Wails CLI is required. Install: go install github.com/wailsapp/wails/v2/cmd/wails@v2.12.0"
}

if (-not (Get-Command makensis -ErrorAction SilentlyContinue)) {
    throw "NSIS is required. Install: winget install NSIS.NSIS"
}

if ($Version) {
    & (Join-Path $PSScriptRoot "inject-version.ps1") -Version $Version
}

$buildVersion = ""
if ($Version) {
    $buildVersion = $Version
    if ($buildVersion -match '^v') {
        $buildVersion = $buildVersion.Substring(1)
    }
}

if (-not $SkipTests) {
    Write-Host "Running tests..."
    go test ./...
    Push-Location (Join-Path $repoRoot "frontend")
    try {
        npm ci
        npm test
    } finally {
        Pop-Location
    }
}

Write-Host "Building application and NSIS installer..."
if ($buildVersion) {
    wails build -platform windows/amd64 -clean -nsis -webview2 embed -ldflags "-X mtgcollection/internal/version.Version=$buildVersion"
} else {
    wails build -platform windows/amd64 -clean -nsis -webview2 embed
}
if ($LASTEXITCODE -ne 0) {
    throw "wails build failed (exit $LASTEXITCODE)"
}

$appBinary = Join-Path $repoRoot "build\bin\mtg-collection.exe"
if (-not (Test-Path $appBinary)) {
    throw "Expected binary at $appBinary"
}

$installer = Get-ChildItem (Join-Path $repoRoot "build\bin") -Filter "*-installer.exe" | Select-Object -First 1
if (-not $installer) {
    throw "Installer not found in build\bin (wails build -nsis should create *-installer.exe)"
}

Write-Host "Release artifacts:"
Write-Host "  $appBinary"
Write-Host "  $($installer.FullName)"
