from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCHEMA_SQL = """
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
"""


@dataclass(frozen=True)
class CardIdentity:
    oracle_id: str
    name: str
    scryfall_uri: str


class CollectionDb:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self._conn.close()

    def upsert_cards(self, cards: Iterable[CardIdentity]) -> None:
        self._conn.executemany(
            """
            INSERT INTO cards (oracle_id, name, scryfall_uri)
            VALUES (?, ?, ?)
            ON CONFLICT(oracle_id) DO UPDATE SET
              name = excluded.name,
              scryfall_uri = excluded.scryfall_uri
            """,
            [(c.oracle_id, c.name, c.scryfall_uri) for c in cards],
        )
        self._conn.commit()

    def increment_collection(self, oracle_id: str, qty: int) -> None:
        if qty <= 0:
            return
        self._conn.execute(
            """
            INSERT INTO collection_items (oracle_id, quantity)
            VALUES (?, ?)
            ON CONFLICT(oracle_id) DO UPDATE SET
              quantity = quantity + excluded.quantity
            """,
            (oracle_id, qty),
        )
        self._conn.commit()

    def set_collection_quantity(self, oracle_id: str, qty: int) -> None:
        if qty < 0:
            raise ValueError("qty must be >= 0")
        self._conn.execute(
            """
            INSERT INTO collection_items (oracle_id, quantity)
            VALUES (?, ?)
            ON CONFLICT(oracle_id) DO UPDATE SET
              quantity = excluded.quantity
            """,
            (oracle_id, qty),
        )
        self._conn.commit()

    def list_collection(self) -> list[sqlite3.Row]:
        cur = self._conn.execute(
            """
            SELECT c.name, ci.quantity, c.scryfall_uri
            FROM collection_items ci
            JOIN cards c ON c.oracle_id = ci.oracle_id
            ORDER BY c.name COLLATE NOCASE
            """
        )
        return list(cur.fetchall())

    def get_owned_quantities_by_name(self) -> dict[str, int]:
        cur = self._conn.execute(
            """
            SELECT c.name, ci.quantity
            FROM collection_items ci
            JOIN cards c ON c.oracle_id = ci.oracle_id
            """
        )
        out: dict[str, int] = {}
        for row in cur.fetchall():
            out[str(row["name"])] = int(row["quantity"])
        return out

    def get_owned_by_oracle_id(self) -> dict[str, tuple[str, int]]:
        cur = self._conn.execute(
            """
            SELECT ci.oracle_id, c.name, ci.quantity
            FROM collection_items ci
            JOIN cards c ON c.oracle_id = ci.oracle_id
            """
        )
        out: dict[str, tuple[str, int]] = {}
        for row in cur.fetchall():
            out[str(row["oracle_id"])] = (str(row["name"]), int(row["quantity"]))
        return out

