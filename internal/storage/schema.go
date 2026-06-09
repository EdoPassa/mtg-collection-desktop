package storage

// SQLite schema: cards are keyed by Scryfall oracle_id; collection_items holds owned quantities;
// deck_cards links decks to cards; lent_cards tracks copies loaned to other players.
const schemaSQL = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS cards (
  oracle_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  scryfall_uri TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  oracle_id TEXT NOT NULL REFERENCES cards(oracle_id) ON DELETE RESTRICT,
  quantity INTEGER NOT NULL CHECK (quantity >= 0),
  notes TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_collection_oracle_id ON collection_items(oracle_id);

CREATE TABLE IF NOT EXISTS decks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS deck_cards (
  deck_id INTEGER NOT NULL REFERENCES decks(id) ON DELETE CASCADE,
  oracle_id TEXT NOT NULL REFERENCES cards(oracle_id) ON DELETE RESTRICT,
  board TEXT NOT NULL DEFAULT 'main' CHECK (board IN ('main', 'side')),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  PRIMARY KEY (deck_id, oracle_id, board)
);

CREATE INDEX IF NOT EXISTS idx_deck_cards_oracle_id ON deck_cards(oracle_id);

CREATE TABLE IF NOT EXISTS lent_cards (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  oracle_id TEXT NOT NULL REFERENCES cards(oracle_id) ON DELETE CASCADE,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  borrower_name TEXT NOT NULL,
  lent_date TEXT NOT NULL,
  return_date TEXT,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_lent_oracle_id ON lent_cards(oracle_id);

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);
INSERT OR IGNORE INTO schema_migrations(version) VALUES (3);
`

const migrateV3SQL = `
CREATE TABLE collection_folders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  parent_id INTEGER REFERENCES collection_folders(id) ON DELETE CASCADE,
  sort_order INTEGER NOT NULL DEFAULT 0,
  UNIQUE (parent_id, name)
);

CREATE TABLE collection_folder_items (
  folder_id INTEGER NOT NULL REFERENCES collection_folders(id) ON DELETE CASCADE,
  oracle_id TEXT NOT NULL REFERENCES cards(oracle_id) ON DELETE RESTRICT,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  PRIMARY KEY (folder_id, oracle_id)
);

CREATE INDEX IF NOT EXISTS idx_folder_items_oracle ON collection_folder_items(oracle_id);
`

const migrateV2SQL = `
CREATE TABLE deck_cards_v2 (
  deck_id INTEGER NOT NULL REFERENCES decks(id) ON DELETE CASCADE,
  oracle_id TEXT NOT NULL REFERENCES cards(oracle_id) ON DELETE RESTRICT,
  board TEXT NOT NULL DEFAULT 'main' CHECK (board IN ('main', 'side')),
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  PRIMARY KEY (deck_id, oracle_id, board)
);
INSERT INTO deck_cards_v2 (deck_id, oracle_id, board, quantity)
SELECT deck_id, oracle_id, 'main', quantity FROM deck_cards;
DROP TABLE deck_cards;
ALTER TABLE deck_cards_v2 RENAME TO deck_cards;
CREATE INDEX IF NOT EXISTS idx_deck_cards_oracle_id ON deck_cards(oracle_id);
`
