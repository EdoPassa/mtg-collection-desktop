# Go Rewrite Compatibility Contract

This contract records the desktop app behavior that the Go implementation must preserve.

## Data Model

- `cards.oracle_id` is the canonical card identity and primary key.
- `collection_items` stores owned quantity per `oracle_id`; quantities are independent from deck quantities.
- `decks` stores unique non-empty deck names.
- `deck_cards` stores positive quantities per `(deck_id, oracle_id)` and replaces all rows when a deck is rebuilt from compare.
- `lent_cards.return_date IS NULL` means the card is still lent.
- Lending records do not decrement collection quantity.

## Import Formats

Text imports accept:

- `4 Lightning Bolt`
- `Lightning Bolt 4`
- `2x Counterspell`

Blank lines and lines beginning with `#` are ignored. Invalid lines are returned as unresolved parse rows.

CSV imports require card name and quantity columns using aliases:

- Name: `name` or `card name`
- Quantity: `quantity` or `qty`
- Optional Scryfall card ID: `scryfall id`

## Resolution

- Normalize names by trimming, collapsing whitespace, and case-folding.
- Use the local Scryfall Oracle bulk index first.
- Resolve by Scryfall card UUID when an import row supplies one.
- Fall back to Scryfall API exact name lookup, then fuzzy name lookup.
- On bootstrap/cache failure, the app remains usable in API-only mode.
- Scryfall HTTP behavior preserves timeout, throttling, retry, 429 `Retry-After`, and 5xx backoff semantics.

## Deck Compare And Build

- Decklists use the same text parser as imports.
- Wanted quantities are grouped by resolved `oracle_id`.
- Owned quantities compare by `oracle_id` first.
- If an owned card has the same normalized name but a different `oracle_id`, compare can use the name fallback and offer a repair.
- Repair moves collection quantity from the stale oracle id to the resolved card oracle id.
- Building a deck from compare is allowed only when there are no unresolved rows and no missing quantities.

## Data Paths

- Development mode keeps compatibility with `data/collection.sqlite3` and `data/scryfall`.
- Packaged mode may use an OS app-data directory, but must offer migration/import for existing repo-local data.
