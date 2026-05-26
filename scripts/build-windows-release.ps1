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

if ($Version) {
    & (Join-Path $PSScriptRoot "inject-version.ps1") -Version $Version
}

$ldflags = ""
if ($Version) {
    if ($Version -match '^v') {
        $Version = $Version.Substring(1)
    }
    $ldflags = "-ldflags `"-X mtgcollection/internal/version.Version=$Version`""
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

Write-Host "Building application binary..."
if ($ldflags) {
    wails build -platform windows/amd64 -clean -webview2 embed $ldflags
} else {
    wails build -platform windows/amd64 -clean -webview2 embed
}

$appBinary = Join-Path $repoRoot "build\bin\mtg-collection.exe"
if (-not (Test-Path $appBinary)) {
    throw "Expected binary at $appBinary"
}

if (-not (Get-Command makensis -ErrorAction SilentlyContinue)) {
    throw "NSIS makensis is required for the installer. Install: winget install NSIS.NSIS"
}

$installerDir = Join-Path $repoRoot "build\windows\installer"
Push-Location $installerDir
try {
    makensis -DARG_WAILS_AMD64_BINARY=..\..\bin\mtg-collection.exe project.nsi
    if ($LASTEXITCODE -ne 0) {
        throw "makensis failed (exit $LASTEXITCODE)"
    }
} finally {
    Pop-Location
}

$installer = Get-ChildItem (Join-Path $repoRoot "build\bin") -Filter "*-installer.exe" | Select-Object -First 1
if (-not $installer) {
    throw "Installer not found in build\bin"
}

Write-Host "Release artifacts:"
Write-Host "  $appBinary"
Write-Host "  $($installer.FullName)"
