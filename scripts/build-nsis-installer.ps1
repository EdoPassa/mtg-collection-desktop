$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "resolve-makensis.ps1")

$repoRoot = Split-Path -Parent $PSScriptRoot
$makensis = Get-MakeNSISPath
Write-Host "Using makensis: $makensis"

$appBinary = Join-Path $repoRoot "build\bin\mtg-collection.exe"
if (-not (Test-Path $appBinary)) {
    throw "Application binary not found: $appBinary (run wails build first)"
}

$installerDir = Join-Path $repoRoot "build\windows\installer"
Push-Location $installerDir
try {
    & $makensis "-DARG_WAILS_AMD64_BINARY=..\..\bin\mtg-collection.exe" project.nsi
    if ($LASTEXITCODE -ne 0) {
        throw "makensis failed (exit $LASTEXITCODE)"
    }
} finally {
    Pop-Location
}

$installer = Get-ChildItem (Join-Path $repoRoot "build\bin") -Filter "*-installer.exe" | Select-Object -First 1
if (-not $installer) {
    throw "Installer not found in build\bin after makensis"
}

Write-Host "Installer: $($installer.FullName)"
