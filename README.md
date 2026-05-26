# MTG Collection Desktop

Local desktop app for tracking a Magic: The Gathering card collection, comparing decklists, and recording cards lent to other players.

Built with **Go**, **Wails v2**, **React**, and **SQLite**. Card names are resolved through Scryfall using a bulk oracle cache when available, with API fallback when the cache is missing or stale.

## Downloads (Windows)

Pre-built installers are published on [GitHub Releases](https://github.com/EdoPassa/mtg-collection-desktop/releases). Download `mtg-collection-amd64-installer.exe` and verify the SHA-256 hash in `checksums.txt`.

Requirements: Windows 10 or later. First launch may need internet access to download the Scryfall oracle bulk cache. Windows SmartScreen may warn on unsigned downloads; verify the checksum before running.

See [`docs/release-windows.md`](docs/release-windows.md) for install details and maintainer release setup.

## Features

- **Import** — Preview and commit card lists from plain text or CSV; unresolved rows are reported before commit.
- **Collection** — Search owned cards; view owned, lent, and available quantities; see whether a card appears in a saved deck.
- **Decks (Library)** — Browse saved decks, rename or delete them, and edit card quantities.
- **Decks (Compare)** — Compare a pasted decklist against your collection, repair oracle ID mismatches, and build a named deck when you have every card.
- **Decks (Analysis)** — Hypergeometric draw odds for a saved deck: specific cards, land counts from Scryfall `type_line` (bulk cache), and a generic calculator with opening-hand presets.
- **Lending** — Record loans by choosing a card from your collection and a borrower; mark active loans as returned.

Resolution uses a local Scryfall oracle bulk index first, then exact and fuzzy API lookups. If bulk bootstrap fails, the app still starts in API-only mode (shown in the sidebar status).

## Requirements

| Tool | Version / notes |
| --- | --- |
| Go | 1.25+ |
| Node.js & npm | For the React frontend |
| Wails CLI | `go install github.com/wailsapp/wails/v2/cmd/wails@latest` |
| Linux GUI libs | Arch / Fedora 41+ / Ubuntu 24.04+: `gtk3`, `webkit2gtk-4.1` (Arch: `pacman -S gtk3 webkit2gtk-4.1`). `wails.json` sets `build:tags` to `webkit2_41`. |
| Network | Needed on first run to download Scryfall bulk data |

## Development

From the repository root:

```bash
npm install --prefix frontend
wails dev
```

`wails dev` runs the Go backend and Vite dev server together. The UI talks to the backend through generated bindings in `frontend/wailsjs/`.

### Packaged builds

**Linux**

```bash
wails build
```

**Windows**

```powershell
# Quick local build (app binary only)
.\scripts\build_windows.ps1

# Installer for release (needs NSIS)
.\scripts\build-windows-release.ps1 -Version 0.1.0
```

Output is under `build/bin/` (`mtg-collection.exe` and `mtg-collection-amd64-installer.exe` when NSIS is installed).

### Backend smoke check

Without the Wails GUI, you can verify bootstrap and resolver status:

```bash
go run ./cmd/mtg-collection
```

## Import formats

### Text

One card per line. Examples:

- `4 Lightning Bolt`
- `Lightning Bolt 4`
- `2x Counterspell`

Empty lines are ignored. Lines starting with `#` are comments.

### CSV

Required columns (header names are case-insensitive):

| Field | Accepted headers |
| --- | --- |
| Card name | `name`, `card name` |
| Quantity | `quantity`, `qty` |

Optional: `scryfall id` for UUID-based resolution.

## Data locations

| Artifact | Development | Packaged app |
| --- | --- | --- |
| SQLite DB | `data/collection.sqlite3` | OS app-data dir (`MTG Collection`) |
| Scryfall bulk cache | `data/scryfall/oracle_cards.json` (or `.json.gz`) + `oracle_cards.meta.json` | Same app-data tree |

On first packaged launch, existing repo-local `data/` may be migrated into the app-data directory. See `docs/go_rewrite_cutover.md` for packaging and QA notes.

## Project layout

```text
main.go, bootstrap.go          # Wails entrypoint and app bootstrap
internal/
  storage/                     # SQLite schema and queries
  importer/                    # TXT/CSV parsing
  scryfall/, resolver/         # Scryfall client and card resolution
  collection/                  # Import, compare, deck, lending workflows
  app/                         # Wails-bound API surface
  appdata/                     # Dev vs packaged paths
frontend/src/                  # React UI (App.tsx)
frontend/wailsjs/              # Generated Wails bindings (commit with UI changes)
cmd/mtg-collection/            # CLI smoke entrypoint; embedded assets for non-wails builds
docs/                          # Architecture, behavior contract, release checklist
testdata/                      # Fixtures for Go tests
```

Contributor docs:

- [`docs/architecture.md`](docs/architecture.md) — module boundaries and data flow
- [`docs/rewrite_contract.md`](docs/rewrite_contract.md) — behavior the implementation must preserve
- [`docs/go_rewrite_cutover.md`](docs/go_rewrite_cutover.md) — packaging and manual QA checklist
- [`docs/release-windows.md`](docs/release-windows.md) — Windows releases and GitHub Actions

## Tests

```bash
go test ./...
npm test --prefix frontend
npm run build --prefix frontend
```

`frontend/wailsjs/` is checked in so TypeScript builds and tests work without running `wails generate` on every clone. Regenerate bindings after changing Go methods exposed to the UI.

## Troubleshooting

- **Bulk cache unavailable** — The app runs with slower API-only resolution; check network and retry later.
- **Import failures** — Confirm line format or CSV headers; preview shows unresolved rows before commit.
- **Scryfall errors** — Verify connectivity; the client retries throttling and transient 5xx responses.
- **Linux build errors** — Install `gtk3` and `webkit2gtk-4.1`; ensure Wails CLI is on your `PATH`.
