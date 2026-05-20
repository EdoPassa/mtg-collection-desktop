package collection

import (
	"context"
	"path/filepath"
	"testing"

	"mtgcollection/internal/cards"
	"mtgcollection/internal/importer"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/scryfall"
	"mtgcollection/internal/storage"
)

type fakeResolver struct {
	cards map[string]scryfall.Card
}

func (f fakeResolver) ResolveLine(ctx context.Context, line importer.ImportLine) (resolver.Result, error) {
	if line.ScryfallID != "" {
		if card, ok := f.cards[line.ScryfallID]; ok {
			return resolver.Result{Card: card, Source: "bulk"}, nil
		}
	}
	return f.ResolveName(ctx, line.Name)
}

func (f fakeResolver) ResolveName(_ context.Context, name string) (resolver.Result, error) {
	card := f.cards[name]
	return resolver.Result{Card: card, Source: "bulk"}, nil
}

func (f fakeResolver) ResolveScryfallID(_ context.Context, id string) (resolver.Result, error) {
	card := f.cards[id]
	return resolver.Result{Card: card, Source: "bulk"}, nil
}

func TestPreviewAndCommitImportIncrementCollection(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\nBad line\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Validated) != 1 || len(preview.Unresolved) != 1 {
		t.Fatalf("preview = %#v, want one valid and one unresolved", preview)
	}

	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}
	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Quantity != 4 || rows[0].Card.Name != "Lightning Bolt" {
		t.Fatalf("collection = %#v, want 4 Lightning Bolt", rows)
	}
}

func TestCompareBuildDeckAndLendingUseCases(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\n2 Counterspell\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}

	compare, err := service.CompareDeck(t.Context(), "4 Lightning Bolt\n1 Counterspell\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(compare.Rows) != 2 || compare.HasUnresolved || compare.Rows[0].Missing != 0 {
		t.Fatalf("compare = %#v, want complete deck", compare)
	}
	deckID, err := service.BuildDeckFromCompare(t.Context(), BuildDeckInput{Name: "Burn", Rows: compare.Rows})
	if err != nil {
		t.Fatal(err)
	}
	decks, err := service.ListDecks(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(decks) != 1 || decks[0].ID != deckID {
		t.Fatalf("decks = %#v, want built deck", decks)
	}
	if err := service.LendCard(t.Context(), storage.LendInput{OracleID: "oracle-bolt", Quantity: 1, BorrowerName: "Alice", LentDate: "2026-05-20"}); err != nil {
		t.Fatal(err)
	}
	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Available != rows[0].Quantity-rows[0].LentQty {
		t.Fatalf("availability not derived from lending: %#v", rows[0])
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	res := fakeResolver{cards: map[string]scryfall.Card{
		"Lightning Bolt": {OracleID: "oracle-bolt", Name: "Lightning Bolt", ScryfallURI: "https://example.test/bolt"},
		"Counterspell":   {OracleID: "oracle-counterspell", Name: "Counterspell", ScryfallURI: "https://example.test/counterspell"},
	}}
	_ = cards.CardIdentity{}
	return New(store, res)
}
