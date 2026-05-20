# Go Rewrite Cutover

## Data Migration

- Development builds continue to use `data/collection.sqlite3` and `data/scryfall`.
- Packaged builds use the OS app-data directory under `MTG Collection`.
- Before any schema-changing migration, create a timestamped copy of the SQLite database.
- First packaged launch should offer to copy repo-local `data/collection.sqlite3` and `data/scryfall` into the app-data directory.

## Windows Packaging

1. Install Go and Node.js.
2. Install Wails CLI with `go install github.com/wailsapp/wails/v2/cmd/wails@latest`.
3. From the repository root, run `wails build`.
4. Verify the generated executable starts without Python installed.
5. Verify the executable can open a migrated copy of an existing database.

## Generated Artifacts

- Commit `frontend/wailsjs/` when the frontend imports generated Wails bindings directly. This keeps TypeScript builds and tests reproducible without requiring every checkout to run `wails generate` first.
- Treat `cmd/mtg-collection/assets/` as build output unless a release process explicitly needs embedded assets committed. If assets are committed, rebuild them with `npm run build --prefix frontend` and verify stale hashed files were removed.
- Keep `frontend/package-lock.json` committed so frontend dependency resolution remains deterministic.

## Manual QA Checklist

- Start with no database and confirm the app creates a usable empty collection.
- Start with an existing Python-created `data/collection.sqlite3` and confirm collection, decks, and lending rows load.
- Import TXT rows with prefix, suffix, and `2x` quantities.
- Import CSV rows with `Card Name`, `Quantity`, and optional `Scryfall ID`.
- Confirm bulk-first resolution works after cache bootstrap.
- Disconnect network or force cache failure and confirm API-only mode is reported without blocking startup.
- Compare a complete decklist and build a deck from it.
- Compare a decklist with missing cards and confirm build is blocked.
- Trigger an oracle-id mismatch and confirm repair merges quantity into the resolved card.
- Lend a card, confirm available quantity changes, then mark it returned.
- Export missing cards CSV and confirm headers are `Card,Needed,Owned,Missing`.
