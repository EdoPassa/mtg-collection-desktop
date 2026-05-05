# MTG Collection Desktop

Desktop application for tracking a Magic: The Gathering collection locally.

The app is built with `PySide6`, persists data in SQLite, and resolves cards through Scryfall. It supports importing lists, comparing decklists against owned cards, building decks, and tracking lent cards.

## What the app does

- Import card lists from plain text or CSV files.
- Validate cards through Scryfall (bulk local index first, API fallback).
- Store and update owned quantities in local SQLite (`data/collection.sqlite3`).
- Compare a decklist against your collection and export missing cards as CSV.
- Build and persist named decks.
- Track lent cards and mark returns.

## Requirements

- Python `3.11+`
- Internet access on first run (to bootstrap Scryfall bulk data cache)

## Quickstart

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
python -m mtg_collection
```

### macOS / Linux

```bash
python -m venv .venv
source .venv/bin/activate
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

- Collection database: `data/collection.sqlite3`
- Scryfall bulk cache:
  - `data/scryfall/oracle_cards.json` or `data/scryfall/oracle_cards.json.gz`
  - `data/scryfall/oracle_cards.meta.json`

On startup, the app attempts to prepare the Scryfall oracle bulk cache in a background thread. If that fails, the UI still starts and continues with API-only lookups.

## Project structure

```text
src/mtg_collection/
  __main__.py          # App entrypoint
  db.py                # SQLite schema and persistence
  importer.py          # TXT/CSV parsing
  resolver.py          # Bulk-first and API-only resolution logic
  scryfall.py          # Scryfall API client with retries/throttling
  scryfall_bulk.py     # Bulk metadata fetch/download and streaming parsers
  ui/                  # Main window, tabs, theme tokens/widgets
tests/
  test_db_decks.py
  test_ui_theme.py
scripts/
  verify_resolver.py   # Resolver smoke check
```

Architecture details for contributors: `docs/architecture.md`.

## Running tests

```bash
python -m unittest discover -s tests
```

Resolver smoke test:

```bash
python scripts/verify_resolver.py
```

## Troubleshooting

- If startup warns that bulk data is unavailable, the app will still run with slower API resolution.
- If imports fail, confirm card names and quantities are valid and that CSV headers match the accepted names.
- If Scryfall requests fail repeatedly, check network connectivity and retry.

