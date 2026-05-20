# Compatibility Matrix

| Area | Behavior To Preserve |
| --- | --- |
| Collection import | Upsert cards, then increment collection quantity by parsed quantity. |
| Collection listing | Ordered by card name case-insensitively and includes `in_deck`. |
| Deck creation | Deck names are trimmed, non-empty, and unique. |
| Deck cards | Adding the same card increments quantity; replacing a deck clears previous rows. |
| Compare | Group wanted cards by resolved oracle id and compute `missing = max(0, needed-owned)`. |
| Compare repair | Move collection quantity from stale oracle id to resolved card oracle id. |
| Lending | Active lending rows have `return_date` null; returned rows remain in history. |
| Availability | Available quantity is displayed as owned quantity minus active lent quantity. |
