# Architecture Notes

This document explains how the MTG Collection Desktop app is organized and how data flows through the main modules.

## Runtime overview

1. `wails dev` or the packaged executable starts the Wails application from `main.go`.
2. `bootstrap()` in `bootstrap.go`:
   - resolves packaged app-data paths through `internal/appdata`;
   - opens SQLite through `internal/storage`;
   - creates the Scryfall client and card resolver.
3. Resolver bootstrap attempts bulk-first mode:
   - ensure Scryfall bulk cache exists and is fresh;
   - build in-memory indexes for name/id lookup;
   - fall back to API-only resolver if bootstrap fails.
4. Wails binds `internal/app.App` methods into `frontend/wailsjs`.
5. `frontend/src/App.tsx` renders the React workflows and calls the generated Wails bindings through a typed adapter.

## Module boundaries

- `internal/storage`
  - owns SQLite schema migration/bootstrap (`schemaSQL`);
  - exposes CRUD-like operations for cards, collection quantities, decks, and lending;
  - central place for persistence constraints and SQL behavior.

- `internal/importer`
  - parses user input into normalized `ImportLine` records;
  - supports both TXT and CSV;
  - returns `(parsed_rows, unresolved_rows)` for caller-side UX messaging.

- `internal/scryfall`
  - low-level Scryfall HTTP client;
  - throttling, retries, backoff, and API response validation;
  - maps responses into minimal `ScryfallCard` identity objects.

- `internal/resolver`
  - high-level card resolution abstraction;
  - bulk-first resolver uses the local Oracle index, then API fallback;
  - API-only resolver is used when cache bootstrap fails.

- `internal/collection`
  - coordinates import preview/commit, deck compare/build, repair, and lending use cases;
  - keeps UI-facing behavior independent from SQLite query details.

- `internal/app`
  - Wails-bound facade over `internal/collection`;
  - converts UI calls into service operations with desktop-safe defaults.

- `internal/appdata`
  - resolves development versus packaged data/cache locations;
  - migrates repo-local data into the app-data directory when needed.

- `frontend/src`
  - React application shell and workflow panels;
  - uses generated `frontend/wailsjs` models/bindings instead of hand-written API shapes.

## Data model

Tables created by `internal/storage`:

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

1. User enters card rows in the React import panel.
2. Wails calls `App.PreviewTextImport` or `App.PreviewCSVImport`.
3. `internal/importer` converts rows to `ImportLine`.
4. `internal/resolver` validates each line to canonical card identity.
5. UI displays validated rows and unresolved entries.
6. Commit step upserts card metadata and increments collection in one transaction block.

## Deck compare flow

1. Decklist text is parsed and resolved to oracle ids.
2. Wanted quantities are compared against owned quantities.
3. If name fallback detects a likely oracle mismatch, UI can trigger repair.
4. Repair migrates collection quantity from old oracle id to resolved oracle id.
5. UI refreshes compare results after repair so stale warnings disappear.

## Tests

- `internal/.../*_test.go`: Go storage, resolver, collection, app-data, and Wails facade behavior.
- `frontend/src/*.test.tsx`: React workflow coverage with mocked Wails APIs.

Run all tests:

```powershell
go test ./...
npm test --prefix frontend
```
