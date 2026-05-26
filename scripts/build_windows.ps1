$ErrorActionPreference = "Stop"

if (-not (Get-Command wails -ErrorAction SilentlyContinue)) {
    Write-Error "Wails CLI is required. Install it with: go install github.com/wailsapp/wails/v2/cmd/wails@v2.12.0"
}

# Local app build. For installer artifacts, use build-windows-release.ps1.
wails build -platform windows/amd64 -webview2 embed
