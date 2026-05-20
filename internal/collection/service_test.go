package collection

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
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

func TestImportPreviewJSONNeverUsesNullSlices(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "not a valid line\n")
	if err != nil {
		t.Fatal(err)
	}
	if preview.Validated == nil || preview.Unresolved == nil {
		t.Fatalf("preview slices must be non-nil: %#v", preview)
	}
	data, err := json.Marshal(preview)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"validated":null`) || strings.Contains(string(data), `"unresolved":null`) {
		t.Fatalf("import preview JSON must use empty arrays, got %s", data)
	}
}

func TestPreviewCSVImportReturnsNonNilSlices(t *testing.T) {
	service := newTestService(t)
	csv := []byte("name,quantity\nLightning Bolt,4\n")
	preview, err := service.PreviewCSVImport(t.Context(), csv)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Validated == nil || preview.Unresolved == nil {
		t.Fatalf("csv preview slices must be non-nil: %#v", preview)
	}
	if len(preview.Validated) != 1 {
		t.Fatalf("validated = %#v, want one row", preview.Validated)
	}
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

func TestListCollectionReturnsEmptySliceForEmptyCollection(t *testing.T) {
	service := newTestService(t)

	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		t.Fatal("ListCollection returned nil, want empty slice for frontend arrays")
	}
	if len(rows) != 0 {
		t.Fatalf("collection = %#v, want empty collection", rows)
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

func TestListCollectionDoesNotReportNegativeAvailability(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "1 Lightning Bolt\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}
	if err := service.LendCard(t.Context(), storage.LendInput{OracleID: "oracle-bolt", Quantity: 2, BorrowerName: "Alice", LentDate: "2026-05-20"}); err != nil {
		t.Fatal(err)
	}

	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("collection = %#v, want one row", rows)
	}
	if rows[0].Available != 0 {
		t.Fatalf("available = %d, want capped zero when lent exceeds owned", rows[0].Available)
	}
}

func TestCompareDeckDoesNotUseAmbiguousNameFallbackAsOwnedQuantity(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldOne := cards.CardIdentity{OracleID: "old-shock-one", Name: "Shock", ScryfallURI: "https://example.test/old-one"}
	oldTwo := cards.CardIdentity{OracleID: "old-shock-two", Name: "Shock", ScryfallURI: "https://example.test/old-two"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{oldOne, oldTwo}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollectionBatch(t.Context(), []storage.QuantityChange{
		{OracleID: oldOne.OracleID, Quantity: 2},
		{OracleID: oldTwo.OracleID, Quantity: 1},
	}); err != nil {
		t.Fatal(err)
	}
	service := New(store, fakeResolver{cards: map[string]scryfall.Card{
		"Shock": {OracleID: "new-shock", Name: "Shock", ScryfallURI: "https://example.test/new"},
	}})

	compare, err := service.CompareDeck(t.Context(), "1 Shock")
	if err != nil {
		t.Fatal(err)
	}
	if len(compare.Rows) != 1 {
		t.Fatalf("compare rows = %#v, want one row", compare.Rows)
	}
	row := compare.Rows[0]
	if row.Owned != 0 || row.Missing != 1 {
		t.Fatalf("ambiguous fallback row = %#v, want owned 0 and missing 1", row)
	}
	if !compare.HasUnresolved || len(compare.Unresolved) == 0 {
		t.Fatalf("compare = %#v, want unresolved ambiguous fallback warning", compare)
	}
	if len(compare.Repairs) != 0 {
		t.Fatalf("repairs = %#v, want no automatic repair for ambiguous fallback", compare.Repairs)
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
