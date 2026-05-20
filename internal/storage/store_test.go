package storage

import (
	"path/filepath"
	"testing"

	"mtgcollection/internal/cards"
)

func TestOpenCreatesCompatibleSchemaAndCollectionWorkflow(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	bolt := cards.CardIdentity{OracleID: "oracle-bolt", Name: "Lightning Bolt", ScryfallURI: "https://example.test/bolt"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{bolt}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollectionBatch(t.Context(), []QuantityChange{{OracleID: bolt.OracleID, Quantity: 4}}); err != nil {
		t.Fatal(err)
	}

	rows, err := store.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Card.OracleID != bolt.OracleID || rows[0].Quantity != 4 || rows[0].InDeck {
		t.Fatalf("collection rows = %#v, want one non-deck bolt row qty 4", rows)
	}
}

func TestListCollectionReturnsEmptySliceForEmptyDatabase(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	rows, err := store.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		t.Fatal("ListCollection returned nil, want empty slice for Wails JSON arrays")
	}
	if len(rows) != 0 {
		t.Fatalf("collection rows = %#v, want empty collection", rows)
	}
}

func TestDeckAndLendingSemantics(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	card := cards.CardIdentity{OracleID: "oracle-counterspell", Name: "Counterspell", ScryfallURI: "https://example.test/counterspell"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{card}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollection(t.Context(), card.OracleID, 4); err != nil {
		t.Fatal(err)
	}

	deckID, err := store.CreateDeck(t.Context(), " Control ")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCardToDeck(t.Context(), deckID, card, 2); err != nil {
		t.Fatal(err)
	}
	if err := store.AddCardToDeck(t.Context(), deckID, card, 1); err != nil {
		t.Fatal(err)
	}

	deckCards, err := store.ListDeckCards(t.Context(), deckID)
	if err != nil {
		t.Fatal(err)
	}
	if len(deckCards) != 1 || deckCards[0].Quantity != 3 {
		t.Fatalf("deck cards = %#v, want one row qty 3", deckCards)
	}

	if err := store.LendCard(t.Context(), LendInput{OracleID: card.OracleID, Quantity: 1, BorrowerName: "Alice", LentDate: "2026-05-20"}); err != nil {
		t.Fatal(err)
	}
	lent, err := store.GetLentSummaryByOracleID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if lent[card.OracleID].TotalQuantity != 1 || lent[card.OracleID].Borrowers[0] != "Alice" {
		t.Fatalf("lent summary = %#v, want Alice qty 1", lent)
	}

	rows, err := store.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !rows[0].InDeck || rows[0].Quantity != 4 {
		t.Fatalf("collection after deck/lend = %#v, want in deck and collection qty unchanged", rows)
	}
}

func TestDeckCRUD(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	card := cards.CardIdentity{OracleID: "oracle-bolt", Name: "Lightning Bolt", ScryfallURI: "https://example.test/bolt"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{card}); err != nil {
		t.Fatal(err)
	}
	deckID, err := store.CreateDeck(t.Context(), "Burn")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCardToDeck(t.Context(), deckID, card, 4); err != nil {
		t.Fatal(err)
	}

	if err := store.SetDeckCardQuantity(t.Context(), deckID, card.OracleID, 2); err != nil {
		t.Fatal(err)
	}
	deckCards, err := store.ListDeckCards(t.Context(), deckID)
	if err != nil {
		t.Fatal(err)
	}
	if len(deckCards) != 1 || deckCards[0].Quantity != 2 {
		t.Fatalf("deck cards after set qty = %#v, want qty 2", deckCards)
	}

	if err := store.SetDeckCardQuantity(t.Context(), deckID, card.OracleID, 0); err != nil {
		t.Fatal(err)
	}
	deckCards, err = store.ListDeckCards(t.Context(), deckID)
	if err != nil {
		t.Fatal(err)
	}
	if len(deckCards) != 0 {
		t.Fatalf("deck cards after remove = %#v, want empty", deckCards)
	}

	if err := store.RenameDeck(t.Context(), deckID, "Mono Red"); err != nil {
		t.Fatal(err)
	}
	decks, err := store.ListDecks(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(decks) != 1 || decks[0].Name != "Mono Red" {
		t.Fatalf("decks after rename = %#v, want Mono Red", decks)
	}

	if err := store.DeleteDeck(t.Context(), deckID); err != nil {
		t.Fatal(err)
	}
	decks, err = store.ListDecks(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(decks) != 0 {
		t.Fatalf("decks after delete = %#v, want empty", decks)
	}
}

func TestMoveCollectionQuantityMergesAndDeletesSource(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	oldCard := cards.CardIdentity{OracleID: "old-oracle", Name: "Shock", ScryfallURI: "https://example.test/old"}
	newCard := cards.CardIdentity{OracleID: "new-oracle", Name: "Shock", ScryfallURI: "https://example.test/new"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{oldCard, newCard}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollection(t.Context(), oldCard.OracleID, 2); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollection(t.Context(), newCard.OracleID, 1); err != nil {
		t.Fatal(err)
	}

	if err := store.MoveCollectionQuantity(t.Context(), oldCard.OracleID, newCard); err != nil {
		t.Fatal(err)
	}

	owned, err := store.GetOwnedByOracleID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := owned[oldCard.OracleID]; ok {
		t.Fatalf("old oracle still present in owned map: %#v", owned)
	}
	if got := owned[newCard.OracleID].Quantity; got != 3 {
		t.Fatalf("merged quantity = %d, want 3", got)
	}
}

func TestMoveCollectionQuantitySameOracleIDPreservesQuantity(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	card := cards.CardIdentity{OracleID: "oracle-shock", Name: "Shock", ScryfallURI: "https://example.test/shock"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{card}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollection(t.Context(), card.OracleID, 2); err != nil {
		t.Fatal(err)
	}

	if err := store.MoveCollectionQuantity(t.Context(), card.OracleID, card); err != nil {
		t.Fatal(err)
	}

	owned, err := store.GetOwnedByOracleID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := owned[card.OracleID].Quantity; got != 2 {
		t.Fatalf("quantity after same-oracle move = %d, want 2", got)
	}
}

func TestReturnCardRejectsInvalidRowsAndDates(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	card := cards.CardIdentity{OracleID: "oracle-opt", Name: "Opt", ScryfallURI: "https://example.test/opt"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{card}); err != nil {
		t.Fatal(err)
	}
	if err := store.LendCard(t.Context(), LendInput{OracleID: card.OracleID, Quantity: 1, BorrowerName: "Alice", LentDate: "2026-05-20"}); err != nil {
		t.Fatal(err)
	}
	lent, err := store.ListLentCards(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lent) != 1 {
		t.Fatalf("lent rows = %#v, want one active row", lent)
	}

	if err := store.ReturnCard(t.Context(), lent[0].ID, ""); err == nil {
		t.Fatal("ReturnCard accepted an empty return date")
	}
	if err := store.ReturnCard(t.Context(), 9999, "2026-05-21"); err == nil {
		t.Fatal("ReturnCard accepted an unknown lending row")
	}
	if err := store.ReturnCard(t.Context(), lent[0].ID, "2026-05-21"); err != nil {
		t.Fatal(err)
	}
	active, err := store.ListLentCards(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active lent rows = %#v, want returned card hidden", active)
	}
}

func TestListLentCardsReturnsEmptySliceForEmptyDatabase(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	rows, err := store.ListLentCards(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		t.Fatal("ListLentCards returned nil, want empty slice for Wails JSON arrays")
	}
	if len(rows) != 0 {
		t.Fatalf("lent rows = %#v, want empty lending list", rows)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}
