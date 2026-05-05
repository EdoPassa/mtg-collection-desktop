# Architecture Notes

This document explains how the MTG Collection Desktop app is organized and how data flows through the main modules.

## Runtime overview

1. `python -m mtg_collection` starts the Qt application from `src/mtg_collection/__main__.py`.
2. `run_app()` in `src/mtg_collection/ui/main_window.py`:
   - applies the global UI theme;
   - opens SQLite through `CollectionDb`;
   - starts resolver bootstrap in a background thread.
3. Resolver bootstrap attempts bulk-first mode:
   - ensure Scryfall bulk cache exists and is fresh;
   - build in-memory indexes for name/id lookup;
   - fall back to API-only resolver if bootstrap fails.
4. `MainWindow` wires five tabs (import, collection, deck compare, deck builder, lent cards) to DB + resolver operations.

## Module boundaries

- `src/mtg_collection/db.py`
  - owns SQLite schema migration/bootstrap (`SCHEMA_SQL`);
  - exposes CRUD-like operations for cards, collection quantities, decks, and lending;
  - central place for persistence constraints and SQL behavior.

- `src/mtg_collection/importer.py`
  - parses user input into normalized `ImportLine` records;
  - supports both TXT and CSV;
  - returns `(parsed_rows, unresolved_rows)` for caller-side UX messaging.

- `src/mtg_collection/scryfall.py`
  - low-level Scryfall HTTP client;
  - throttling, retries, backoff, and API response validation;
  - maps responses into minimal `ScryfallCard` identity objects.

- `src/mtg_collection/scryfall_bulk.py`
  - fetches bulk metadata from `/bulk-data`;
  - downloads and caches oracle cards payload to `data/scryfall/`;
  - streams JSON/JSON.GZ efficiently (uses `ijson` when available).

- `src/mtg_collection/resolver.py`
  - high-level card resolution abstraction (`CardResolver`);
  - `BulkFirstResolver`: in-memory bulk index first, then API fallback;
  - `ApiOnlyResolver`: direct API path used as fallback mode.

- `src/mtg_collection/ui/`
  - `main_window.py` coordinates app state and tab actions;
  - `tabs/*` builds tab-specific widgets and accessible controls;
  - `theme/*` centralizes design tokens + helper widget styling.

## Data model

Tables created by `CollectionDb`:

- `cards`: canonical identity (`oracle_id`, `name`, `scryfall_uri`)
- `collection_items`: owned quantity per card
- `decks`: user-defined deck names
- `deck_cards`: quantity of each card in each deck
- `lent_cards`: lending history and return state

Design notes:

- Card identity is anchored on `oracle_id`.
- Collection quantities and deck quantities are intentionally independent.
- Lending records do not auto-decrement collection quantities.

## Import and validation flow

1. User selects import mode (TXT or CSV file).
2. Parser converts rows to `ImportLine`.
3. Resolver validates each line to canonical card identity.
4. UI displays validated rows and unresolved entries.
5. Commit step upserts card metadata and increments collection in one transaction block.

## Deck compare flow

1. Decklist text is parsed and resolved to oracle ids.
2. Wanted quantities are compared against owned quantities.
3. If name fallback detects a likely oracle mismatch, UI can trigger repair.
4. Repair migrates collection quantity from old oracle id to resolved oracle id.
5. Result table can be filtered (`All cards` / `Missing cards`) and exported as CSV.

## Tests

- `tests/test_db_decks.py`: DB behavior for decks and in-deck status derivation.
- `tests/test_ui_theme.py`:
  - design token and stylesheet checks;
  - tab builder import/accessibility checks;
  - `MainWindow` smoke coverage for baseline tab wiring.

Run all tests:

```bash
python -m unittest discover -s tests
```
