# Windows releases

End users install **MTG Collection** from [GitHub Releases](https://github.com/EdoPassa/mtg-collection-desktop/releases). Maintainers cut releases with a git tag; CI builds and uploads the installer.

Windows may show SmartScreen warnings for unsigned downloads. Users can choose **More info → Run anyway** after verifying the SHA-256 hash in `checksums.txt`.

## User install

1. Download `mtg-collection-amd64-installer.exe` from the latest release (verify with `checksums.txt`).
2. Run the installer (requires administrator approval for per-machine install).
3. On first launch, allow network access so Scryfall oracle bulk data can download (or the app runs in slower API-only mode).
4. Data is stored under `%AppData%\MTG Collection\` (`collection.sqlite3` and `scryfall\` cache).

Portable use: `mtg-collection.exe` in the same release is the app binary without the installer.

## Maintainer: cut a release

1. Complete manual QA from [`go_rewrite_cutover.md`](go_rewrite_cutover.md) on a release build.
2. Tag and push:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
3. Watch the **Release Windows** workflow. It will:
   - Inject `v0.1.0` into [`wails.json`](../wails.json) and [`frontend/package.json`](../frontend/package.json)
   - Run tests, then `wails build -nsis` (app + installer)
   - Publish `mtg-collection-amd64-installer.exe`, `mtg-collection.exe`, and `checksums.txt` to GitHub Releases

### Manual workflow run

Actions → **Release Windows** → **Run workflow**. Artifacts upload to the workflow run when the ref is not a `v*` tag; tagged pushes publish a GitHub Release.

### Local release build

```powershell
.\scripts\build-windows-release.ps1 -Version 0.1.0
```

Requires Wails CLI and NSIS (`winget install NSIS.NSIS`). Use `-SkipTests` when iterating.

## Build pipeline

1. `wails build -nsis` compiles the app, writes `build/windows/installer/wails_tools.nsh` from `wails.json`, and runs `makensis` on [`build/windows/installer/project.nsi`](../build/windows/installer/project.nsi).
2. Outputs land in `build/bin/` (`mtg-collection.exe` and `mtg-collection-amd64-installer.exe`).
3. SHA-256 checksums are written to `checksums.txt` for release assets.

`wails_tools.nsh` is gitignored and must not be committed; it is recreated on every release build.

## Scryfall

The app identifies as `MTGCollectionDesktop/<version>` (see [`internal/version`](../internal/version/version.go)). Follow [Scryfall API guidelines](https://scryfall.com/docs/api).
