function Get-MakeNSISPath {
    $command = Get-Command makensis -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    $searchRoots = @(
        "${env:ProgramFiles(x86)}\NSIS",
        "$env:ProgramFiles\NSIS"
    )

    if ($env:ChocolateyInstall) {
        $searchRoots += @(
            "$env:ChocolateyInstall\bin",
            "$env:ChocolateyInstall\lib\nsis"
        )
    }

    foreach ($root in $searchRoots) {
        if (-not $root -or -not (Test-Path $root)) {
            continue
        }
        $direct = Join-Path $root "makensis.exe"
        if (Test-Path $direct) {
            return (Resolve-Path $direct).Path
        }
        $found = Get-ChildItem -Path $root -Recurse -Filter makensis.exe -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($found) {
            return $found.FullName
        }
    }

    throw "makensis.exe not found. Install NSIS (winget install NSIS.NSIS or choco install nsis) and ensure makensis.exe is available."
}
