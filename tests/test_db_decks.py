from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mtg_collection.db import CardIdentity, CollectionDb


class DeckPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = CollectionDb(Path(self.tmpdir.name) / "collection.sqlite3")

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def test_create_deck_requires_unique_non_empty_name(self) -> None:
        deck_id = self.db.create_deck("Burn")

        self.assertEqual(self.db.list_decks()[0]["id"], deck_id)
        self.assertEqual(self.db.list_decks()[0]["name"], "Burn")
        with self.assertRaises(ValueError):
            self.db.create_deck("  ")
        with self.assertRaises(ValueError):
            self.db.create_deck("Burn")

    def test_add_card_to_deck_increments_existing_deck_quantity(self) -> None:
        deck_id = self.db.create_deck("Izzet")
        card = CardIdentity(
            oracle_id="oracle-bolt",
            name="Lightning Bolt",
            scryfall_uri="https://example.test/lightning-bolt",
        )

        self.db.add_card_to_deck(deck_id, card, 2)
        self.db.add_card_to_deck(deck_id, card, 1)

        rows = self.db.list_deck_cards(deck_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["oracle_id"], "oracle-bolt")
        self.assertEqual(rows[0]["name"], "Lightning Bolt")
        self.assertEqual(rows[0]["quantity"], 3)

    def test_replace_deck_cards_clears_existing_cards_before_inserting(self) -> None:
        deck_id = self.db.create_deck("Burn")
        old_card = CardIdentity(
            oracle_id="oracle-bolt",
            name="Lightning Bolt",
            scryfall_uri="https://example.test/lightning-bolt",
        )
        new_card = CardIdentity(
            oracle_id="oracle-rift-bolt",
            name="Rift Bolt",
            scryfall_uri="https://example.test/rift-bolt",
        )
        self.db.add_card_to_deck(deck_id, old_card, 4)

        self.db.replace_deck_cards(deck_id, [(new_card, 3)])

        rows = self.db.list_deck_cards(deck_id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["oracle_id"], "oracle-rift-bolt")
        self.assertEqual(rows[0]["quantity"], 3)

    def test_collection_in_deck_is_derived_from_saved_deck_cards(self) -> None:
        card = CardIdentity(
            oracle_id="oracle-counterspell",
            name="Counterspell",
            scryfall_uri="https://example.test/counterspell",
        )
        self.db.upsert_cards([card])
        self.db.increment_collection("oracle-counterspell", 4)

        before = self.db.list_collection()
        self.assertEqual(before[0]["in_deck"], 0)

        deck_id = self.db.create_deck("Control")
        self.db.add_card_to_deck(deck_id, card, 2)

        after = self.db.list_collection()
        self.assertEqual(after[0]["in_deck"], 1)


if __name__ == "__main__":
    unittest.main()
