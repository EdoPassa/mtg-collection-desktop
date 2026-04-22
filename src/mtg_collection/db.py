from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import re


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
"""


@dataclass(frozen=True)
class CardIdentity:
    oracle_id: str
    name: str
    scryfall_uri: str


_WS_RE = re.compile(r"\s+")


def _normalize_name(name: str) -> str:
    return _WS_RE.sub(" ", name.strip()).casefold()


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
            SELECT ci.oracle_id, c.name, ci.quantity, c.scryfall_uri
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

    def get_owned_by_normalized_name(self) -> dict[str, list[tuple[str, str, int]]]:
        """
        Returns a mapping from normalized card name -> list of (oracle_id, name, quantity).

        Normalization is conservative: trim, collapse whitespace, casefold.
        """
        cur = self._conn.execute(
            """
            SELECT ci.oracle_id, c.name, ci.quantity
            FROM collection_items ci
            JOIN cards c ON c.oracle_id = ci.oracle_id
            """
        )
        out: dict[str, list[tuple[str, str, int]]] = {}
        for row in cur.fetchall():
            oracle_id = str(row["oracle_id"])
            name = str(row["name"])
            qty = int(row["quantity"])
            key = _normalize_name(name)
            if not key:
                continue
            out.setdefault(key, []).append((oracle_id, name, qty))
        return out

    def move_collection_quantity(self, *, from_oracle_id: str, to_card: CardIdentity) -> None:
        """
        Move (merge) collection quantity from one oracle_id to another.

        - Ensures destination card exists in `cards`
        - Adds source quantity onto destination quantity (if present)
        - Removes source row from `collection_items`
        """
        from_oracle_id = str(from_oracle_id)
        if not from_oracle_id:
            return

        self.upsert_cards([to_card])

        cur = self._conn.execute(
            "SELECT quantity FROM collection_items WHERE oracle_id = ?",
            (from_oracle_id,),
        )
        row = cur.fetchone()
        if row is None:
            return
        qty = int(row["quantity"])
        if qty <= 0:
            self._conn.execute("DELETE FROM collection_items WHERE oracle_id = ?", (from_oracle_id,))
            self._conn.commit()
            return

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO collection_items (oracle_id, quantity)
                VALUES (?, ?)
                ON CONFLICT(oracle_id) DO UPDATE SET
                  quantity = quantity + excluded.quantity
                """,
                (to_card.oracle_id, qty),
            )
            self._conn.execute("DELETE FROM collection_items WHERE oracle_id = ?", (from_oracle_id,))

    def lend_card(self, *, oracle_id: str, quantity: int, borrower_name: str, lent_date: str, notes: str = "") -> None:
        """
        Record that cards have been lent to someone.
        
        - Adds a new entry in the lent_cards table
        - Does NOT automatically reduce collection quantity (user manages this separately if desired)
        """
        if quantity <= 0:
            raise ValueError("quantity must be > 0")
        if not borrower_name.strip():
            raise ValueError("borrower_name cannot be empty")
        
        self._conn.execute(
            """
            INSERT INTO lent_cards (oracle_id, quantity, borrower_name, lent_date, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (oracle_id, quantity, borrower_name.strip(), lent_date, notes),
        )
        self._conn.commit()

    def return_card(self, *, lent_id: int, return_date: str) -> None:
        """
        Mark a lent card as returned by setting its return_date.
        """
        self._conn.execute(
            """
            UPDATE lent_cards
            SET return_date = ?
            WHERE id = ?
            """,
            (return_date, lent_id),
        )
        self._conn.commit()

    def get_lent_cards(self, include_returned: bool = False) -> list[sqlite3.Row]:
        """
        Get all lent cards, optionally including those that have been returned.
        
        Returns rows with: id, oracle_id, card_name, quantity, borrower_name, lent_date, return_date, notes
        """
        if include_returned:
            cur = self._conn.execute(
                """
                SELECT lc.id, lc.oracle_id, c.name AS card_name, lc.quantity, 
                       lc.borrower_name, lc.lent_date, lc.return_date, lc.notes
                FROM lent_cards lc
                JOIN cards c ON c.oracle_id = lc.oracle_id
                ORDER BY lc.lent_date DESC
                """
            )
        else:
            cur = self._conn.execute(
                """
                SELECT lc.id, lc.oracle_id, c.name AS card_name, lc.quantity, 
                       lc.borrower_name, lc.lent_date, lc.return_date, lc.notes
                FROM lent_cards lc
                JOIN cards c ON c.oracle_id = lc.oracle_id
                WHERE lc.return_date IS NULL
                ORDER BY lc.lent_date DESC
                """
            )
        return list(cur.fetchall())

    def get_lent_summary_by_oracle_id(self) -> dict[str, tuple[int, list[str]]]:
        """
        Get a summary of lent quantities per oracle_id.
        
        Returns: {oracle_id: (total_lent_quantity, [borrower_names])}
        Only includes currently lent (not returned) cards.
        """
        cur = self._conn.execute(
            """
            SELECT oracle_id, SUM(quantity) as total_qty, GROUP_CONCAT(borrower_name, ', ') as borrowers
            FROM lent_cards
            WHERE return_date IS NULL
            GROUP BY oracle_id
            """
        )
        out: dict[str, tuple[int, list[str]]] = {}
        for row in cur.fetchall():
            oracle_id = str(row["oracle_id"])
            total_qty = int(row["total_qty"])
            borrowers_str = str(row["borrowers"]) if row["borrowers"] else ""
            borrowers = [b.strip() for b in borrowers_str.split(",") if b.strip()]
            out[oracle_id] = (total_qty, borrowers)
        return out

