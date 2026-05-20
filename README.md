# MTG Collection Desktop

Desktop application for tracking a Magic: The Gathering collection locally.

The active desktop app is a Go/Wails application with a React frontend. It persists data in SQLite and resolves cards through Scryfall using a bulk-first resolver with API fallback. The older Python/PySide6 implementation remains in the repository as legacy/reference code during the rewrite.

## What the app does

- Import card lists from plain text or CSV files.
- Validate cards through Scryfall (bulk local index first, API fallback).
- Store and update owned quantities in local SQLite (`data/collection.sqlite3`).
- Compare a decklist against your collection and export missing cards as CSV.
- Build and persist named decks.
- Track lent cards and mark returns.

## Requirements

- Go `1.25+`
- Node.js and npm
- Wails CLI for packaged builds:
  `go install github.com/wailsapp/wails/v2/cmd/wails@latest`
- Linux (Arch, Fedora 41+, Ubuntu 24.04+): `gtk3` and `webkit2gtk-4.1` (e.g. `pacman -S gtk3 webkit2gtk-4.1` on Arch). `wails.json` sets `build:tags` to `webkit2_41` for these systems.
- Python `3.11+`
- Internet access on first run (to bootstrap Scryfall bulk data cache)

## Quickstart

### Go/Wails Desktop

```powershell
npm install --prefix frontend
wails dev
```

Build a Windows executable:

```powershell
.\scripts\build_windows.ps1
```

Build a Linux binary:

```bash
wails build
```

### Legacy Python App

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
python -m mtg_collection
```

## Import formats

### Text import

Accepted examples:

- `4 Lightning Bolt`
- `Lightning Bolt 4`
- `2x Counterspell`

Notes:

- Empty lines are ignored.
- Lines starting with `#` are treated as comments.

### CSV import

The CSV file must include:

- Card name: `name` or `card name`
- Quantity: `quantity` or `qty`

Optional:

- Scryfall card id: `scryfall id`

## Application tabs

- `Import`: validate TXT/CSV input and add resolved cards to collection.
- `Collection`: search/sort owned cards, show lent quantity, and open quick lend dialog.
- `Deck compare`: compare pasted decklist vs owned quantities, repair known oracle id mismatches, export filtered results.
- `Deck builder`: create decks and add resolved cards with quantities.
- `Lent cards`: add lending records, filter returned cards, mark cards as returned.

## Data and cache

- Development database: `data/collection.sqlite3`
- Development Scryfall bulk cache:
  - `data/scryfall/oracle_cards.json` or `data/scryfall/oracle_cards.json.gz`
  - `data/scryfall/oracle_cards.meta.json`
- Packaged Wails builds use the OS app-data directory under `MTG Collection`.

On startup, the app attempts to prepare the Scryfall oracle bulk cache. If that fails, the UI still starts and continues with API-only lookups.

## Project structure

```text
main.go                         # Wails entrypoint
bootstrap.go                    # Packaged app bootstrap
internal/                       # Go storage, resolver, collection service, app bindings
frontend/src/                   # React UI
frontend/wailsjs/               # Generated Wails bindings used by the frontend
cmd/mtg-collection/             # CLI smoke entrypoint and embedded build assets
src/mtg_collection/             # Legacy Python/PySide6 implementation
```

Architecture details for contributors: `docs/architecture.md`.

## Running tests

```powershell
go test ./...
npm test --prefix frontend
npm run build --prefix frontend
```

Legacy Python tests:

```bash
python -m unittest discover -s tests
```

## Troubleshooting

- If startup warns that bulk data is unavailable, the app will still run with slower API resolution.
- If imports fail, confirm card names and quantities are valid and that CSV headers match the accepted names.
- If Scryfall requests fail repeatedly, check network connectivity and retry.

