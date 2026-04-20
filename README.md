# MTG Collection Desktop (MVP)

Python desktop app to import MTG card lists (TXT/CSV), validate via Scryfall, and store a local collection in SQLite.

## Run

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pip install -e .
python -m mtg_collection
```

## CSV format (MVP)

The CSV importer expects a header row with:
- `name`
- `quantity` (or `qty`)

